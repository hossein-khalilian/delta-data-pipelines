from datetime import datetime, timedelta
import json
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import redis
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from kafka import KafkaProducer, KafkaConsumer

from curl2json.parser import parse_curl

# crawler
USER_AGENT_DEFAULT = "DivarTokenCrawler/1.0 (+https://example.com)"

# Redis
REDIS_HOST = "172.16.36.111"
REDIS_PORT = 6379
REDIS_BLOOM_FILTER = "divar_tokens_bloom_1"

# Kafka
KAFKA_BOOTSTRAP_SERVERS = ["172.16.36.111:9092"]
KAFKA_TOPIC = "divar_tokens1"

# MongoDB
MONGO_URI = "mongodb://appuser:appassword@172.16.36.111:27017/delta-datasets"
MONGO_DB = "delta-datasets"
MONGO_COLLECTION = "crawl.1"

# API endpoint
DIVAR_API_URL = "https://api.divar.ir/v8/posts-v2/web/{}"

# ETL for crawler DAG
def extract_tokens(**kwargs):
    BLOOM_KEY = REDIS_BLOOM_FILTER  
    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT) 
    
    try:
        rdb.execute_command("BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000)
    except Exception as e:
        print(f"⚠️ خطا در ایجاد Bloom filter: {e}")


    curl_command = """curl 'https://api.divar.ir/v8/postlist/w/search' \
      --compressed \
      -X POST \
      --data-raw '{"city_ids":["1"],"pagination_data":{"@type":"type.googleapis.com/post_list.PaginationData","page":0,"layer_page":0,"search_bookmark_info":{"alert_state":{}}},"search_data":{"form_data":{"data":{"category":{"str":{"value":"apartment-sell"}}}},"server_payload":{"@type":"type.googleapis.com/widgets.SearchData.ServerPayload","additional_form_data":{"data":{"sort":{"str":{"value":"sort_date"}}}}}}}'"""
    
    parsed_curl = parse_curl(curl_command)
    parsed_curl.pop("cookies", None)
    
    client_params = {
        "verify": True,
        "headers": parsed_curl.pop("headers", {}),
    }
    
    all_tokens = set()
    max_pages = 100
    
    with httpx.Client(**client_params) as client:
        # GET for get Cookies
        try:
            resp = client.get("https://divar.ir")
            resp.raise_for_status()
            print("✅ Cookies دریافت شد")
        except Exception as e:
            print(f"❌ خطا در گرفتن cookies: {e}")
            return


        curl_data = json.loads(parsed_curl.get("data"))

        for page in range(max_pages):
            try:
                # به‌روزرسانی pagination_data برای صفحه فعلی
                curl_data["pagination_data"]["page"] = page
                curl_data["pagination_data"]["layer_page"] = 0
                parsed_curl["data"] = json.dumps(curl_data)
                
                # ارسال درخواست POST
                response = client.request(
                    method=parsed_curl.get("method", "POST"),
                    url=parsed_curl["url"],
                    headers=parsed_curl.get("headers", {}),
                    content=parsed_curl.get("data"),
                    params=parsed_curl.get("params")
                )
                response.raise_for_status()
                result = response.json()
            
                # استخراج توکن‌ها
                widgets = result.get("list_widgets", []) or []
                tokens = [w.get("data", {}).get("token") for w in widgets if w.get("data", {}).get("token")]
                if not tokens:
                    print(f"⛔️ صفحه {page}: هیچ توکنی یافت نشد، توقف.")
                    break
                
                for t in tokens:
                    print(f"🔹 توکن یافت شد: {t}")
                    
                print(f"📄 صفحه {page}: {result.get('list_widgets')[0].get('data').get('title')}")
                print(f"📊 تعداد آگهی‌ها: {len(widgets)}")
                
                # چک کردن توکن‌های تکراری با Bloom filter
                duplicate_count, new_tokens = 0, []
                for token in tokens:
                    exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, token)
                    if exists:
                        duplicate_count += 1
                    else:
                        new_tokens.append(token)
                        rdb.execute_command("BF.ADD", BLOOM_KEY, token)

                all_tokens.update(new_tokens)
                ratio = duplicate_count / len(tokens) if tokens else 1
                print(f"📊 صفحه {page}: {duplicate_count}/{len(tokens)} تکراری ({ratio:.0%})")
            
                if ratio >= 0.3:
                    print(f"🛑 صفحه {page}: بیش از 30درصد تکراری — توقف.")
                    break
            
                # update pagination_data 
                pagination_info = result.get("pagination", {}) or {}
                curl_data["pagination_data"] = pagination_info.get("data", curl_data["pagination_data"])
                
                time.sleep(1.5)   

            except Exception as e:
                print(f"❌ خطا در درخواست صفحه {page}: {e}")
                break


    kwargs["ti"].xcom_push(key="extracted_tokens", value=list(all_tokens))
    print(f"✅ استخراج کامل شد — {len(all_tokens)} توکن جدید ارسال شد به XCom.")
    
def filter_tokens(**kwargs):
    tokens = kwargs['ti'].xcom_pull(key='extracted_tokens', task_ids='extract_tokens') or []
    if not tokens:
        print("هیچ توکنی برای فیلتر کردن وجود ندارد.")
        kwargs['ti'].xcom_push(key='filtered_tokens', value=[])
        return

    # r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    # new_tokens = []
    # for token in tokens:
    #     exists = r.execute_command("BF.EXISTS", REDIS_BLOOM_FILTER, token)
    #     if not exists:
    #         r.execute_command("BF.ADD", REDIS_BLOOM_FILTER, token)
    #         new_tokens.append(token)

    kwargs['ti'].xcom_push(key='filtered_tokens', value=tokens)
    print(f"انتقال یافت: {len(tokens)} توکن به XCom")

def produce_to_kafka(**kwargs):
    tokens = kwargs['ti'].xcom_pull(key='filtered_tokens', task_ids='filter_tokens')
    if not tokens:
        print("هیچ توکن جدیدی برای ارسال به کافکا وجود ندارد.")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for token in tokens:
        producer.send(KAFKA_TOPIC, token)
    producer.flush()
    print(f"ارسال شد: {len(tokens)} توکن به کافکا")

# ETL for fetch DAG 
def transform_json_to_doc(data: dict) -> dict:
    doc = {}
    doc["record_timestamp"] = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    doc["cat2_slug"] = data.get("analytics", {}).get("cat2") or "null"
    doc["cat3_slug"] = data.get("analytics", {}).get("cat3") or "null"
    city_data = data.get("city")
    if isinstance(city_data, dict):
        doc["city_slug"] = city_data.get("second_slug", "null")
    else:
        doc["city_slug"] = city_data or "null"
    doc["neighborhood_slug"] = data.get("webengage", {}).get("district") or "null"
    raw_date = data.get("seo", {}).get("unavailable_after")
    doc["created_at_month"] = None
    if raw_date:
        try:
            dt = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            doc["created_at_month"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    raw_user_type = data.get("webengage", {}).get("business_type")
    mapping = {"personal": "شخصی", "premium-panel": "مشاور املاک"}
    doc["user_type"] = mapping.get(raw_user_type, float("nan"))
    doc["description"] = (
        data.get("seo", {}).get("post_seo_schema", {}).get("description") or "null"
    )
    doc["title"] = data.get("seo", {}).get("web_info", {}).get("title") or "null"
    doc["rent_mode"] = "null"
    doc["rent_value"] = "null"
    doc["rent_to_single"] = "null"
    doc["rent_type"] = "null"
    doc["price_mode"] = "null"
    doc["price_value"] = "null"
    doc["credit_mode"] = "null"
    doc["credit_value"] = "null"
    doc["rent_credit_transform"] = "null"
    doc["transformable_price"] = "null"
    doc["transformable_credit"] = "null"
    doc["transformed_credit"] = "null"
    doc["transformable_rent"] = "null"
    doc["transformed_rent"] = "null"
    list_data = next(
        (s for s in data.get("sections", []) if s.get("section_name") == "LIST_DATA"),
        {},
    )
    widgets = list_data.get("widgets", [])
    breadcrumb = next(
        (s for s in data.get("sections", []) if s.get("section_name") == "BREADCRUMB"),
        {},
    )
    breadcrumb_widget = next(
        (
            w
            for w in breadcrumb.get("widgets", [])
            if w.get("widget_type") == "BREADCRUMB"
        ),
        None,
    )
    current_page_title = (
        breadcrumb_widget.get("data", {}).get("current_page_title", "")
        if breadcrumb_widget
        else ""
    )
    if "رایگان" in current_page_title or "مجانی" in current_page_title:
        doc["price_mode"] = "مجانی"
    elif "توافقی" in current_page_title:
        doc["price_mode"] = "توافقی"
    elif "مقطوع" in current_page_title:
        doc["price_mode"] = "مقطوع"
    price_widget = next(
        (
            w
            for w in widgets
            if w.get("widget_type") == "UNEXPANDABLE_ROW"
            and w.get("data", {}).get("title") == "قیمت کل"
        ),
        None,
    )
    if price_widget:
        value = price_widget.get("data", {}).get("value", "null")
        doc["price_value"] = value.replace(" تومان", "") if value != "null" else "null"
    physical_fields = [
        "land_size",
        "building_size",
        "deed_type",
        "has_business_deed",
        "floor",
        "rooms_count",
        "total_floors_count",
        "unit_per_floor",
    ]
    for field in physical_fields:
        doc[field] = "null"
    group_feature_row = next(
        (w for w in widgets if w.get("widget_type") == "GROUP_FEATURE_ROW"), None
    )
    modal_features = []
    if group_feature_row:
        modal_features = (
            group_feature_row.get("data", {})
            .get("action", {})
            .get("payload", {})
            .get("modal_page", {})
            .get("widget_list", [])
            or []
        )
    description = next(
        (
            w.get("data", {}).get("text", "")
            for s in data.get("sections", [])
            if s.get("section_name") == "DESCRIPTION"
            for w in s.get("widgets", [])
            if w.get("widget_type") == "DESCRIPTION_ROW"
        ),
        "",
    )
    for widget in widgets:
        if (
            widget.get("widget_type") == "UNEXPANDABLE_ROW"
            and widget.get("data", {}).get("title") == "متراژ زمین"
        ):
            doc["land_size"] = widget.get("data", {}).get("value", "null")
            break
    for widget in widgets:
        if widget.get("widget_type") == "GROUP_INFO_ROW":
            items = widget.get("data", {}).get("items", []) or []
            for item in items:
                title = item.get("title", "")
                value = item.get("value", "")
                if "متراژ" in title:
                    doc["building_size"] = value
                    break
            if doc["building_size"] != "null":
                break
    deed_type_map = {
        "تک‌برگ": "single_page",
        "منگوله‌دار": "single_page",
        "قول‌نامه‌ای": "written_agreement",
        "نامشخص": "unselect",
        "unselect": "unselect",
        "سایر": "other",
    }
    for widget in widgets:
        if (
            widget.get("widget_type") == "UNEXPANDABLE_ROW"
            and widget.get("data", {}).get("title") == "سند"
        ):
            raw_deed_type = widget.get("data", {}).get("value", None)
            doc["deed_type"] = (
                deed_type_map.get(raw_deed_type, "null") if raw_deed_type else "null"
            )
            break
    else:
        raw_deed_type = next(
            (
                m.get("data", {}).get("value")
                for m in modal_features
                if m.get("data", {}).get("title") == "سند"
            ),
            None,
        )
        doc["deed_type"] = (
            deed_type_map.get(raw_deed_type, "null") if raw_deed_type else "null"
        )
    doc["has_business_deed"] = "null"
    floor_map = {"همکف": "0", "هم‌کف": "0"}
    for widget in widgets:
        if (
            widget.get("widget_type") == "UNEXPANDABLE_ROW"
            and widget.get("data", {}).get("title") == "طبقه"
        ):
            raw_floor = widget.get("data", {}).get("value", "null")
            if raw_floor != "null":
                if raw_floor in floor_map:
                    doc["floor"] = floor_map[raw_floor]
                else:
                    match = re.search(r"(\d+)\s*از\s*(\d+)", raw_floor)
                    if match:
                        doc["floor"] = match.group(1)
                    else:
                        try:
                            float(raw_floor)
                            doc["floor"] = raw_floor
                        except (ValueError, TypeError):
                            doc["floor"] = "null"
            break
    for widget in widgets:
        if widget.get("widget_type") == "GROUP_INFO_ROW":
            items = widget.get("data", {}).get("items", []) or []
            for item in items:
                title = item.get("title", "")
                value = item.get("value", "")
                if "اتاق" in title:
                    doc["rooms_count"] = value
                    break
            if doc["rooms_count"] != "null":
                break
    for widget in widgets:
        if (
            widget.get("widget_type") == "UNEXPANDABLE_ROW"
            and widget.get("data", {}).get("title") == "طبقه"
        ):
            floor_value = widget.get("data", {}).get("value", "null")
            if floor_value != "null":
                match = re.search(r"(\d+)\s*از\s*(\d+)", floor_value)
                if match:
                    doc["total_floors_count"] = match.group(2)
                    break
    if doc["total_floors_count"] == "null" and description:
        match = re.search(r"(\d+)\s*از\s*(\d+)", description)
        if match:
            doc["total_floors_count"] = match.group(2)
    doc["unit_per_floor"] = next(
        (
            m.get("data", {}).get("value")
            for m in modal_features
            if m.get("data", {}).get("title") == "تعداد واحد در طبقه"
        ),
        "null",
    )
    features_map = {
        "آسانسور": "has_elevator",
        "پارکینگ": "has_parking",
        "انباری": "has_warehouse",
        "بالکن": "has_balcony",
        "سرمایش داکت اسپلیت": "has_cooling_system",
        "گرمایش داکت اسپلیت": "has_heating_system",
        "تأمین‌کننده آب گرم پکیج": "has_warm_water_provider",
        "آب": "has_water",
        "برق": "has_electricity",
        "گاز": "has_gas",
        "نگهبان": "has_security_guard",
        "باربیکیو": "has_barbecue",
        "استخر": "has_pool",
        "جکوزی": "has_jacuzzi",
        "سونا": "has_sauna",
    }
    floor_material_map = {
        "جنس کف سنگ": "stone",
        "جنس کف سرامیک": "ceramic",
        "جنس کف موکت": "carpet",
        "جنس کف پارکت چوبی": "wood_parquet",
        "جنس کف موزاییک": "mosaic",
        "جنس کف پارکت لمینت": "laminate_parquet",
        "جنس کف پوشش کف": "floor_covering",
    }
    warm_water_provider_map = {
        "تأمین‌کننده آب گرم پکیج": "package",
        "تأمین‌کننده آب گرم آبگرمکن": "water_heater",
        "تأمین‌کننده آب گرم موتورخانه": "powerhouse",
    }
    cooling_system_map = {
        "سرمایش کولر گازی": "split",
        "سرمایش کولر آبی": "water_cooler",
        "سرمایش داکت اسپلیت": "duct_split",
        "سرمایش اسپلیت": "split",
        "سرمایش فن کویل": "fan_coil",
        "سرمایش هواساز": "air_conditioner",
    }
    heating_system_map = {
        "گرمایش شوفاژ": "shoofaj",
        "گرمایش داکت اسپلیت": "duct_split",
        "گرمایش بخاری": "heater",
        "گرمایش اسپلیت": "split",
        "گرمایش شومینه": "fireplace",
        "گرمایش از کف": "floor_heating",
        "گرمایش فن کویل": "fan_coil",
    }
    restroom_map = {
        "سرویس بهداشتی ایرانی و فرنگی": "squat_seat",
        "سرویس بهداشتی ایرانی": "squat",
        "سرویس بهداشتی فرنگی": "seat",
    }
    property_type_map = {
        "ویلای ساحلی": "beach",
        "ویلای جنگلی": "jungle",
        "ویلای کوهستانی": "mountain",
        "ویلای جنگلی-کوهستانی": "jungle-mountain",
        "سایر": "other",
    }
    building_direction_map = {
        "شمالی": "north",
        "جنوبی": "south",
        "شرقی": "east",
        "غربی": "west",
        "نامشخص": "unselect",
    }
    all_feature_fields = [
        "has_balcony",
        "has_elevator",
        "has_warehouse",
        "has_parking",
        "construction_year",
        "is_rebuilt",
        "has_water",
        "has_warm_water_provider",
        "has_electricity",
        "has_gas",
        "has_heating_system",
        "has_cooling_system",
        "has_restroom",
        "has_security_guard",
        "has_barbecue",
        "building_direction",
        "has_pool",
        "has_jacuzzi",
        "has_sauna",
        "floor_material",
        "property_type",
    ]
    for f in all_feature_fields:
        doc[f] = "null"
    if group_feature_row:
        for it in group_feature_row.get("data", {}).get("items", []) or []:
            title = it.get("title", "") or ""
            available = it.get("available")
            for k, v in features_map.items():
                if k in title:
                    if "ندارد" in title:
                        doc[v] = False
                    elif available is not None:
                        doc[v] = bool(available)
                    else:
                        doc[v] = True
    for m in modal_features:
        mdata = m.get("data", {}) or {}
        title = mdata.get("title", "") or mdata.get("text", "") or ""
        for k, v in features_map.items():
            if k in title:
                if "ندارد" in title:
                    doc[v] = False
                else:
                    doc[v] = True
        if m.get("widget_type") == "UNEXPANDABLE_ROW" and title == "وضعیت واحد":
            doc["is_rebuilt"] = mdata.get("value", "null") == "بازسازی شده"
        if m.get("widget_type") == "UNEXPANDABLE_ROW" and title == "جهت ساختمان":
            doc["building_direction"] = building_direction_map.get(
                mdata.get("value", "unselect"), "unselect"
            )
        if "کف" in title:
            doc["floor_material"] = floor_material_map.get(title, "unselect")
        if "تأمین‌کننده آب گرم" in title:
            doc["has_warm_water_provider"] = warm_water_provider_map.get(
                title, "unselect"
            )
        if "سرمایش" in title:
            doc["has_cooling_system"] = cooling_system_map.get(title, "unselect")
        if "سرویس بهداشتی" in title:
            doc["has_restroom"] = restroom_map.get(title, "unselect")
        if m.get("widget_type") == "FEATURE_ROW" and "گرمایش" in title:
            doc["has_heating_system"] = heating_system_map.get(title, "unselect")
    for section in data.get("sections", []):
        if section.get("section_name") == "LIST_DATA":
            for widget in section.get("widgets", []):
                if widget.get("widget_type") == "GROUP_INFO_ROW":
                    for item in widget.get("data", {}).get("items", []):
                        title = item.get("title", "") or ""
                        if title == "ساخت":
                            doc["construction_year"] = item.get("value", "null")
                if widget.get("widget_type") == "UNEXPANDABLE_ROW":
                    mdata = widget.get("data", {}) or {}
                    title = mdata.get("title", "") or ""
                    if title == "نوع ملک":
                        doc["property_type"] = property_type_map.get(
                            mdata.get("value", ""), "other"
                        )
    doc["regular_person_capacity"] = "null"
    doc["extra_person_capacity"] = "null"
    doc["cost_per_extra_person"] = "null"
    doc["rent_price_on_regular_days"] = "null"
    doc["rent_price_on_special_days"] = "null"
    doc["rent_price_at_weekends"] = "null"
    lat = None
    lon = None
    radius = "null"
    seo_geo = data.get("seo", {}).get("post_seo_schema", {}).get("geo", {}) or {}
    lat = seo_geo.get("latitude") or seo_geo.get("lat") or None
    lon = seo_geo.get("longitude") or seo_geo.get("lng") or seo_geo.get("long") or None
    if not lat or not lon:
        map_section = next(
            (s for s in data.get("sections", []) if s.get("section_name") == "MAP"), {}
        )
        map_widgets = map_section.get("widgets", []) or []
        map_widget = next(
            (w for w in map_widgets if w.get("data", {}).get("location")), None
        )
        if map_widget:
            location = map_widget.get("data", {}).get("location", {}) or {}
            fuzzy = location.get("fuzzy_data") or {}
            exact = location.get("exact_data") or {}
            if fuzzy:
                center = fuzzy.get("point") or fuzzy.get("center") or {}
                lat = center.get("latitude") or center.get("lat") or lat
                lon = center.get("longitude") or center.get("lng") or lon
                radius = fuzzy.get("radius") or fuzzy.get("r") or "null"
            elif exact:
                lat = exact.get("latitude") or exact.get("lat") or lat
                lon = exact.get("longitude") or exact.get("lng") or lon
                radius = "null"
            else:
                radius = location.get("radius", "null")
    doc["location_latitude"] = str(lat) if lat is not None else "null"
    doc["location_longitude"] = str(lon) if lon is not None else "null"
    doc["location_radius"] = radius if radius is not None else "null"
    images = []
    schema_images = data.get("seo", {}).get("post_seo_schema", {}).get("image")
    if isinstance(schema_images, list):
        images.extend([i for i in schema_images if i])
    elif schema_images:
        images.append(schema_images)
    for section in data.get("sections", []) or []:
        if section.get("section_name") == "IMAGE":
            for widget in section.get("widgets", []) or []:
                if widget.get("widget_type") == "IMAGE_CAROUSEL":
                    for item in widget.get("data", {}).get("items", []) or []:
                        img = item.get("image", {}).get("url")
                        if img:
                            images.append(img)
    doc["images"] = list(dict.fromkeys(images))
    return doc

class KafkaMessageSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic = KAFKA_TOPIC

    def poke(self, context):
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                # auto_offset_reset="latest",
                auto_offset_reset="earliest",
                group_id="divar_sensor_group",
                enable_auto_commit=False,
            )
            messages = consumer.poll(timeout_ms=5000)
            has_messages = any(len(records) > 0 for records in messages.values())
            consumer.close()

            if has_messages:
                print(f"✅ پیام جدیدی در تاپیک '{self.topic}' پیدا شد.")
            else:
                print(f"⚠️ هیچ پیامی در تاپیک '{self.topic}' یافت نشد. منتظر پیام جدید می‌مانم...")

            return has_messages

        except Exception as e:
            print(f"❌ خطا در بررسی پیام‌های Kafka: {e}")
            return False
    
def consume_and_fetch(**kwargs):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="divar_consumer_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    messages = consumer.poll(timeout_ms=10000, max_records=1)
    consumer.commit()
    consumer.close()

    for topic_partition, partition_messages in messages.items():
        for message in partition_messages:
            token = message.value
            url = DIVAR_API_URL.format(token)
            try:
                with httpx.Client() as client:
                    response = client.get(url, headers={"User-Agent": USER_AGENT_DEFAULT})
                    response.raise_for_status()
                    data = response.json()
                    kwargs['ti'].xcom_push(key='fetched_data', value=data)
                    kwargs['ti'].xcom_push(key='token', value=token)
                    print(f"دریافت شد: داده برای توکن {token}")
                    return
            except Exception as e:
                print(f"خطا در دریافت محتوای {token}: {e}")
                return
    print("هیچ پیامی در کافکا یافت نشد.")

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='fetched_data', task_ids='consume_and_fetch')
    token = kwargs['ti'].xcom_pull(key='token', task_ids='consume_and_fetch')
    if not data:
        print(f"هیچ داده‌ای برای تبدیل وجود ندارد برای توکن {token}.")
        return

    try:
        transformed = transform_json_to_doc(data)
        transformed["post_token"] = token
        transformed["crawl_timestamp"] = datetime.utcnow().isoformat()
        kwargs['ti'].xcom_push(key='transformed_data', value=transformed)
        print(f"تبدیل شد: داده برای توکن {token}")
    except Exception as e:
        print(f"خطا در تبدیل JSON برای {token}: {e}")

def store_to_mongo(**kwargs):
    transformed = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    token = kwargs['ti'].xcom_pull(key='token', task_ids='consume_and_fetch')
    if not transformed:
        print(f"هیچ داده‌ای برای ذخیره در MongoDB وجود ندارد برای توکن {token}.")
        return

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    try:
        collection.create_index("post_token", unique=True)
        collection.insert_one(transformed)
        print(f"ذخیره شد: داده برای توکن {token} در MongoDB")
    except DuplicateKeyError:
        print(f"تکراری: توکن {token} قبلاً ذخیره شده است.")
    except Exception as e:
        print(f"خطا در ذخیره {token}: {e}")
    finally:
        client.close()

# DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

producer_dag = DAG(
    "divar_crawler1",
    default_args=default_args,
    description="استخراج 100 توکن دیوار، فیلتر با بلوم، و ارسال به کافکا هر 5 دقیقه",
    schedule_interval="*/5 * * * *",
    catchup=False,
)

consumer_dag = DAG(
    "divar_fetch1",
    default_args=default_args,
    description="مصرف یک توکن از کافکا، دریافت، تبدیل و ذخیره در MongoDB هر 5 دقیقه",
    schedule_interval="*/5 * * * *",
    catchup=False,
)

# --- تسک‌های DAG تولیدکننده ---
extract_task = PythonOperator(
    task_id="extract_tokens",
    python_callable=extract_tokens,
    provide_context=True,
    dag=producer_dag,
)

filter_task = PythonOperator(
    task_id="filter_tokens",
    python_callable=filter_tokens,
    provide_context=True,
    dag=producer_dag,
)

produce_task = PythonOperator(
    task_id="produce_to_kafka",
    python_callable=produce_to_kafka,
    provide_context=True,
    dag=producer_dag,
)

#  گراف DAG تولیدکننده
extract_task >> filter_task >> produce_task

# --- تسک‌های DAG مصرف‌کننده ---
kafka_sensor = KafkaMessageSensor(
    task_id="kafka_message_sensor",
    poke_interval=60,
    timeout=600,
    dag=consumer_dag,
)

consume_fetch_task = PythonOperator(
    task_id="consume_and_fetch",
    python_callable=consume_and_fetch,
    provide_context=True,
    dag=consumer_dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=consumer_dag,
)

store_task = PythonOperator(
    task_id="store_to_mongo",
    python_callable=store_to_mongo,
    provide_context=True,
    dag=consumer_dag,
)

#  گراف DAG مصرف‌کننده
kafka_sensor >> consume_fetch_task >> transform_task >> store_task