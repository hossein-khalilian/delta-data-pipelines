from datetime import datetime, timedelta
import json
import time

import httpx
from curl2json.parser import parse_curl
import redis

from kafka import KafkaProducer, KafkaConsumer

from config import config 

# ETL for crawler DAG
def extract_tokens(**kwargs):
    BLOOM_KEY = config["redis_bloom_filter"]  
    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"]) 
    
    # بررسی وجود Bloom filter
    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command("BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000)
            print(f"✅ Bloom filter با نام {BLOOM_KEY} ایجاد شد")
        except Exception as e:
            print(f"⚠️ خطا در ایجاد Bloom filter: {e}")
    else:
        print(f"✅ Bloom filter با نام {BLOOM_KEY} وجود دارد")

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
                
                # for t in tokens:
                #     print(f"🔹 توکن یافت شد: {t}")
                
                # print(f"📄 صفحه {page}: {result.get('list_widgets')[0].get('data').get('title')}")
                print(f"📊 تعداد آگهی‌ها: {len(widgets)}")
                print(f" صفحه : {page}")
                      
                # for w in widgets:
                #     print(f"🔹 توکن یافت شد: {w}")

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
        bootstrap_servers=config["kafka_bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for token in tokens:
        producer.send(config["kafka_topic"], token)
    producer.flush()
    print(f"ارسال شد: {len(tokens)} توکن به کافکا")
