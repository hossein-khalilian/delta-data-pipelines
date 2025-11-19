import httpx
from pymongo import MongoClient
from datetime import datetime
import math
import re

# MongoDB connection details
MONGO_URI = "mongodb://appuser:appassword@172.16.36.111:27017/delta-datasets"
MONGO_DB = "delta-datasets"
MONGO_COLLECTION = "shey.test1"

# Helper: Clean string to slug (e.g. "تهران" -> "tehran")
def to_slug(text: str) -> str:
    if not text:
        return None
    return re.sub(r'\s+', '-', text.strip().lower())

# Helper: Extract float from price string like "2,700,000,000"
def parse_price(price_str: str) -> float:
    if not price_str:
        return None
    cleaned = re.sub(r'[^\d]', '', price_str)
    return float(cleaned) if cleaned else None

# Main extraction function
def extract_from_json(item: dict) -> dict:
    doc = {}

    attributes = item.get("attributes", {})
    full_attrs = item.get("fullAttributes", [])
    geo = item.get("geo", {})


    # --- cat2_slug & cat3_slug ---
    categories = attributes.get("categories", [])
    doc["cat2_slug"] = to_slug(categories[0].get("name")) if len(categories) > 0 else None
    doc["cat3_slug"] = to_slug(categories[1].get("name")) if len(categories) > 1 else None

    # --- Location ---
    location_str = attributes.get("location")
    if location_str:
        parts = [p.strip() for p in location_str.split("،")]
        doc["city_slug"] = to_slug(parts[0]) if len(parts) > 0 else None
        doc["neighborhood_slug"] = to_slug(parts[1]) if len(parts) > 1 else None
    else:
        doc["city_slug"] = None
        doc["neighborhood_slug"] = None

    # --- created_at_month ---
    doc["created_at_month"] = datetime.now().strftime("%Y-%m-01 00:00:00")

    # --- user_type ---
    doc["user_type"] = None

    # --- Title & Description ---
    doc["description"] = item.get("description") or None
    doc["title"] = attributes.get("title") or None

    # --- Price, Rent, Credit ---
    doc["rent_mode"] = None
    doc["rent_value"] = None
    doc["rent_to_single"] = None
    doc["rent_type"] = None
    doc["price_mode"] = None
    doc["price_value"] = None
    doc["credit_mode"] = None
    doc["credit_value"] = None
    doc["rent_credit_transform"] = None
    doc["transformable_price"] = None
    doc["transformable_credit"] = None
    doc["transformed_credit"] = None
    doc["transformable_rent"] = None
    doc["transformed_rent"] = None

    # --- Full Attributes Mapping ---
    attr_map = {attr.get("key"): attr.get("value") for attr in full_attrs if attr.get("key") and attr.get("value")}

    # --- Price & Rent/Credit from price list ---
    price_list = attributes.get("price", [])
    if price_list:
        first_price = price_list[0]
        label = first_price.get("label", "").strip()
        amount = parse_price(first_price.get("amount"))
        if label in ["رهن", "رهن کامل"]:
            doc["credit_value"] = amount or None
            doc["credit_mode"] = "مقطوع"
        elif label == "اجاره":
            doc["rent_value"] = amount or None
            doc["rent_mode"] = "مقطوع"
        else:
            doc["price_value"] = amount or None
            doc["price_mode"] = "مقطوع"

    # --- Rent & Credit from fullAttributes ---
    if "رهن" in attr_map:
        doc["credit_value"] = parse_price(attr_map["رهن"]) or None
        doc["credit_mode"] = "مقطوع"
    if "اجاره" in attr_map:
        doc["rent_value"] = parse_price(attr_map["اجاره"]) or None
        doc["rent_mode"] = "مقطوع"

    # --- rent_type logic ---
    rent_val = doc["rent_value"]
    credit_val = doc["credit_value"]
    if rent_val is not None and credit_val is not None and rent_val > 0 and credit_val > 0:
        doc["rent_type"] = "rent_credit"
    elif rent_val == 0 and credit_val is not None and credit_val > 0:
        doc["rent_type"] = "full_credit"
    else:
        doc["rent_type"] = None

    # --- Rent/Credit transform ---
    doc["rent_credit_transform"] = attr_map.get("قابلیت تبدیل مبلغ رهن و اجاره") == "true" or None

    # --- Size ---
    doc["land_size"] = None
    size_str = attr_map.get("متراژ")
    doc["building_size"] = float(size_str) if size_str and size_str.isdigit() else None

    # --- Deed Type ---
    deed_attr = next((attr for attr in full_attrs if attr.get("key") == "نوع سند"), None)
    doc["deed_type"] = deed_attr.get("value") if deed_attr else None

    # --- has_business_deed (مثلاً اگر "تجاری" بود) ---
    doc["has_business_deed"] = doc["deed_type"] == "تجاری" if doc["deed_type"] else None

    # --- Floor ---
    floor_attr = next((attr for attr in full_attrs if attr.get("key") == "طبقه ملک"), None)
    floor_val = floor_attr.get("value") if floor_attr else None
    doc["floor"] = int(floor_val) if floor_val and floor_val.isdigit() else None

    # --- Rooms ---
    rooms_str = attr_map.get("تعداد اتاق")
    doc["rooms_count"] = int(rooms_str) if rooms_str and rooms_str.isdigit() else None

    # --- Total floors ---
    doc["total_floors_count"] = None

    # --- Unit per floor ---
    unit_per_floor_attr = next((attr for attr in full_attrs if attr.get("key") == "تعداد واحد در طبقه"), None)
    unit_val = unit_per_floor_attr.get("value") if unit_per_floor_attr else None
    doc["unit_per_floor"] = int(unit_val) if unit_val and unit_val.isdigit() else None

    # --- Facilities ---
    doc["has_balcony"] = attr_map.get("بالکن") == "دارد" if "بالکن" in attr_map else None
    doc["has_elevator"] = attr_map.get("آسانسور") == "دارد" or None
    doc["has_warehouse"] = attr_map.get("انباری") == "دارد" or None
    doc["has_parking"] = attr_map.get("پارکینگ") == "دارد" or None

    # --- Construction year ---
    year_str = attr_map.get("سال ساخت بنا")
    doc["construction_year"] = int(year_str) if year_str and year_str.isdigit() else None

    # --- Other fields ---
    doc["is_rebuilt"] = None
    doc["has_water"] = None
    doc["has_warm_water_provider"] = None
    doc["has_electricity"] = None
    doc["has_gas"] = None
    doc["has_heating_system"] = None
    doc["has_cooling_system"] = None
    doc["has_restroom"] = None
    doc["has_security_guard"] = None
    doc["has_barbecue"] = None
    doc["building_direction"] = None
    doc["has_pool"] = None
    doc["has_jacuzzi"] = None
    doc["has_sauna"] = None
    doc["floor_material"] = None
    doc["property_type"] = attr_map.get("نوع ملک") or None

    # --- Short-term rental ---
    # doc["regular_person_capacity"] = None
    doc["extra_person_capacity"] = None
    doc["cost_per_extra_person"] = None
    # doc["rent_price_on_regular_days"] = None
    doc["rent_price_on_special_days"] = None
    doc["rent_price_at_weekends"] = None
    
    capacity_attr = next((attr for attr in full_attrs if attr.get("key") == "ظرفیت"), None)
    doc["regular_person_capacity"] = int(capacity_attr.get("value")) if capacity_attr and capacity_attr.get("value", "").isdigit() else None

    daily_rent_attr = next((attr for attr in full_attrs if attr.get("key") == "اجاره روزانه"), None)
    daily_rent_val = daily_rent_attr.get("value") if daily_rent_attr else None
    doc["rent_price_on_regular_days"] = parse_price(daily_rent_val) or None
    
    # --- Geo ---
    doc["location_latitude"] = geo.get("lat") or None
    doc["location_longitude"] = geo.get("lon") or None
    doc["location_radius"] = 500.0 if geo else None

    return doc

# Fetch JSON with error handling (like Divar example)
def fetch_json(post_url: str) -> dict:
    try:
        r = httpx.get(post_url, timeout=10.0)
        if r.status_code == 404:
            print(f"Post not found {post_url}")
            return None
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as e:
        print(f"Error fetching {post_url}: {e}")
        return None


# Main function to process list of URLs
def json_to_mongo(urls):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    inserted = 0
    for url in urls:
        json_data = fetch_json(url)
        if not json_data or "data" not in json_data:
            print(f"No valid data from {url}")
            continue

        items = json_data["data"]
        for item in items:
            if item.get("type") in ["paidEngagement", "normal"]:
                doc = extract_from_json(item)
                collection.insert_one(doc)
                inserted += 1

    print(f"Inserted {inserted} documents into {MONGO_COLLECTION}")
    client.close()


# Example usage
if __name__ == "__main__":
    urls = [
        "https://www.sheypoor.com/api/v10.0.0/search/tehran/real-estate?f=%5B0.0%2C%201762862520490%5D_457359670n&p=2",
        # Add more pages if needed
    ]
    json_to_mongo(urls)