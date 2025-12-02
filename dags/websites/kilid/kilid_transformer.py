import re
from datetime import datetime, timedelta
from typing import List, Dict, Any
from bs4 import BeautifulSoup

def persian_to_english_digits(text: str) -> str:
    if not text:
        return ""
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    english_digits = "0123456789"
    trans_table = str.maketrans(persian_digits, english_digits)
    return text.translate(trans_table)

def text_to_date(text: str) -> datetime | None:
    if not text:
        return None

    text = text.strip()
    text = persian_to_english_digits(text)
    now = datetime.now()

    match = re.search(r"(\d+)", text)
    amount = int(match.group(1)) if match else 0

    if "دقیقه" in text:
        return now - timedelta(minutes=amount)
    elif "ساعت" in text:
        return now - timedelta(hours=amount)
    elif "روز" in text:
        return now - timedelta(days=amount)
    elif "هفته" in text:
        return now - timedelta(weeks=amount)
    elif "ماه" in text:
        return now - timedelta(days=amount * 30)
    else:
        return now  

def clean_text(text: str) -> str:
    return text.replace("\u200c", " ").strip() if text else ""

def transformer_function(fetched_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transformed_results = []

    feature_map = {
        "متراژ": "building_size",
        "زیربنا": "building_size",
        "متراژ بنا": "building_size",
        "متراژ زمین": "land_size",
        "زمین": "land_size",
        "طبقه": "floor",
        "تعداد طبقات": "total_floors_count",
        "تعداد اتاق": "rooms_count",
        "تعداد واحد در طبقه": "unit_per_floor",
        "سال ساخت": "construction_year",
        "نوسازی شده": "is_rebuilt",

        "پارکینگ": "has_parking",
        "انباری": "has_warehouse",
        "آسانسور": "has_elevator",
        "بالکن": "has_balcony",

        "سند": "deed_type",
        "نوع سند": "deed_type",
        "سند تجاری": "has_business_deed",

        "آب": "has_water",
        "برق": "has_electricity",
        "گاز": "has_gas",
        "آب گرم": "has_warm_water_provider",

        "سیستم گرمایش": "has_heating_system",
        "سیستم سرمایش": "has_cooling_system",

        "سرویس بهداشتی": "has_restroom",
        "نگهبان": "has_security_guard",
        "باربیکیو": "has_barbecue",

        "جهت ساختمان": "building_direction",
        "جهت بنا": "building_direction",

        "استخر": "has_pool",
        "جکوزی": "has_jacuzzi",
        "سونا": "has_sauna",

        "جنس کف": "floor_material",

        "ظرفیت پایه": "regular_person_capacity",
        "ظرفیت اضافه": "extra_person_capacity",
        "هزینه هر نفر اضافه": "cost_per_extra_person",

        "قیمت ایام عادی": "rent_price_on_regular_days",
        "قیمت ایام خاص": "rent_price_on_special_days",
        "قیمت آخر هفته": "rent_price_at_weekends",

        "lat": "location_latitude",
        "lng": "location_longitude",
        "radius": "location_radius",
    }

    def map_feature(key, value, out):
        if key in feature_map:
            out[feature_map[key]] = persian_to_english_digits(value)

    for item in fetched_data:
        html = item.get("html_content")
        url = item.get("content_url")

        if not html or not url:
            print(f"Skipping invalid item: {url}")
            continue

        soup = BeautifulSoup(html, "html.parser")

        try:
            # Breadcrumbs
            breadcrumbs = [a.get_text(strip=True) for a in soup.select('nav[aria-label="breadcrumb"] a')]
            cat2_slug = breadcrumbs[1] if len(breadcrumbs) > 1 else None
            cat3_slug = breadcrumbs[2] if len(breadcrumbs) > 2 else None
            city_slug = breadcrumbs[3] if len(breadcrumbs) > 3 else None
            neighborhood_slug = breadcrumbs[4] if len(breadcrumbs) > 4 else None
            gallery_imgs = [
                img["src"]
                for img in soup.select("div.relative img")
                if img.get("src")
            ]
            # Ad code & publish time
            ad_code = None
            published_time_raw = None
            for div in soup.select('div.flex.items-center.justify-center.gap-1'):
                text = div.get_text()
                if "کد آگهی" in text:
                    span = div.select_one('span.font-semiBold')
                    ad_code = span.get_text(strip=True) if span else None
                if "انتشار" in text:
                    span = div.select_one('span.font-semiBold')
                    published_time_raw = span.get_text(strip=True) if span else None

            published_time = text_to_date(published_time_raw)

            # Title
            title_tag = soup.select_one('h1.text-lg.font-semibold')
            title = clean_text(title_tag.get_text()) if title_tag else None

            # Price
            price_tags = soup.select('div.flex-row.items-center.justify-between span.text-lg.font-bold')
            price_total = persian_to_english_digits(price_tags[0].get_text(strip=True)) if len(price_tags) > 0 else None
            # rent
            rent_credit_blocks = soup.select('div.flex.flex-col.items-end.justify-center span.text-lg.font-bold')

            credit_value = None
            rent_value = None

            if len(rent_credit_blocks) >= 2:
                credit_value = persian_to_english_digits(
                    clean_text(rent_credit_blocks[0].get_text(strip=True))
                )
                rent_value = persian_to_english_digits(
                    clean_text(rent_credit_blocks[1].get_text(strip=True))
                )

            # Document type
            doc_div = soup.select_one('div.inline-flex.items-center.bg-gray-50')
            document_type = clean_text(doc_div.get_text()) if doc_div else None

            # features
            raw_features = {}
            for div in soup.select('div.group.flex.items-center.justify-center'):
                key_span = div.select_one('span.text-nowrap')
                if key_span:
                    key = clean_text(key_span.get_text())
                    value_text = div.get_text().replace(key, "", 1).strip()
                    raw_features[key] = persian_to_english_digits(clean_text(value_text))

            raw_additional = {}
            container = soup.select_one('div.grid.w-full.grid-cols-2.gap-x-6.gap-y-4.text-right.text-sm.text-gray-900.md\\:grid-cols-3')
            if container:
                for div in container.select('div.flex.justify-start.gap-2'):
                    key_span = div.select_one('span.text-gray-600')
                    value_span = div.select_one('span.font-semiBold')
                    if key_span and value_span:
                        key = clean_text(key_span.get_text().replace(":", ""))
                        value = persian_to_english_digits(clean_text(value_span.get_text()))
                        raw_additional[key] = value

            # description
            desc_div = soup.find("div", class_=re.compile(r"transition-all\s+duration-300"))
            description = clean_text(desc_div.get_text(separator="\n")) if desc_div else None

            out = {
                "created_at": datetime.now(),

                "cat2_slug": cat2_slug,
                "cat3_slug": cat3_slug,
                "city_slug": city_slug,
                "neighborhood_slug": neighborhood_slug,
                "created_at_month": datetime.now().strftime("%Y-%m"),

                "user_type": raw_features.get("نوع آگهی"),

                "description": description,
                "title": title,

                "rent_mode": raw_features.get("نوع اجاره"),
                "rent_value": raw_features.get("مبلغ اجاره"),
                "rent_to_single": raw_features.get("اجاره به مجرد"),
                "rent_type": raw_features.get("نحوه اجاره"),

                "price_mode": raw_features.get("نوع قیمت"),
                "price_value": price_total,
                "credit_value": credit_value,
                "rent_value": rent_value,

                "credit_mode": raw_features.get("نوع رهن"),
                "credit_value": raw_features.get("مبلغ رهن"),

                "rent_credit_transform": None,
                "transformable_price": None,
                "transformable_credit": None,
                "transformed_credit": None,
                "transformable_rent": None,
                "transformed_rent": None,

                # defaults for ALL mapped fields (ensures field always exists)
                "land_size": None,
                "building_size": None,
                "deed_type": document_type,
                "has_business_deed": None,
                "floor": None,
                "rooms_count": None,
                "total_floors_count": None,
                "unit_per_floor": None,
                "has_balcony": None,
                "has_elevator": None,
                "has_warehouse": None,
                "has_parking": None,
                "construction_year": None,
                "is_rebuilt": None,
                "has_water": None,
                "has_warm_water_provider": None,
                "has_electricity": None,
                "has_gas": None,
                "has_heating_system": None,
                "has_cooling_system": None,
                "has_restroom": None,
                "has_security_guard": None,
                "has_barbecue": None,
                "building_direction": None,
                "has_pool": None,
                "has_jacuzzi": None,
                "has_sauna": None,
                "floor_material": None,
                "property_type": raw_features.get("نوع ملک"),

                "regular_person_capacity": None,
                "extra_person_capacity": None,
                "cost_per_extra_person": None,
                "rent_price_on_regular_days": None,
                "rent_price_on_special_days": None,
                "rent_price_at_weekends": None,

                "location_latitude": item.get("lat"),
                "location_longitude": item.get("lng"),
                "location_radius": item.get("radius"),

                "images": gallery_imgs,
                "content_url": url,
            }

            # Map features into out
            for k, v in raw_features.items():
                map_feature(k, v, out)

            for k, v in raw_additional.items():
                map_feature(k, v, out)

            transformed_results.append(out)

        except Exception as e:
            print(f"Error transforming {url}: {e}")
            transformed_results.append({
                "content_url": url,
                "source": "kilid",
                "error": str(e),
                "created_at": datetime.now().isoformat(),
            })

    print(f"Transformer completed: {len(transformed_results)} items")
    return transformed_results
