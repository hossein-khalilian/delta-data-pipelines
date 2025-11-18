import re
from datetime import datetime

def transformer_function(fetched_data):
    if not fetched_data:
        print("⚠️No data for transformation.")
        return []

    transformed_data = []
    for item in fetched_data:
        url = item["content_url"]
        data = item["data"]
        try:
            transformed = transform_data(data)
            transformed["content_url"] = url
            transformed_data.append(transformed)
        except Exception as e:
            print(f"Error converting JSON for {url}: {e}")
            continue
        
    print(f"✅ Transformed {len(transformed_data)} items.")
    return transformed_data

def to_slug(text: str) -> str:
    if not text:
        return None
    return re.sub(r'\s+', '-', text.strip().lower().replace("،", ",").split(",")[0])

def parse_price(price_str: str) -> float:
    if not price_str or not isinstance(price_str, str):
        return None
    cleaned = re.sub(r'[^\d]', '', price_str)
    return float(cleaned) if cleaned else None

def transform_data(raw_data: dict) -> dict:

    item = raw_data.get("data", {}) if "data" in raw_data else raw_data  

    doc = {
        "created_at": datetime.now(),
        "cat2_slug": None,
        "cat3_slug": None,
        "city_slug": None,
        "neighborhood_slug": None,
        "created_at_month": datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        "user_type": "شخصی",  
        "description": item.get("description") or None,
        "title": item.get("title") or None,
        "content_url": None,
    }

    attributes = item.get("attributes", {})
    full_attrs = item.get("fullAttributes", [])
    geo = item.get("geo", {})

    # categories
    categories = attributes.get("categories", [])
    if len(categories) > 0:
        doc["cat2_slug"] = to_slug(categories[0].get("name"))
    if len(categories) > 1:
        doc["cat3_slug"] = to_slug(categories[1].get("name"))

    # location
    location_str = attributes.get("location")
    if location_str:
        parts = [p.strip() for p in location_str.replace("،", ",").split(",")]
        doc["city_slug"] = to_slug(parts[0]) if len(parts) > 0 else None
        doc["neighborhood_slug"] = to_slug(parts[1]) if len(parts) > 1 else None

    # map for fullAttributes
    attr_map = {attr.get("key"): attr.get("value") for attr in full_attrs if attr.get("key")}

    # credit, rent , price
    price_list = attributes.get("price", [])
    if price_list:
        first = price_list[0]
        label = first.get("label", "").strip()
        amount = parse_price(first.get("amount"))
        if label in ["رهن", "رهن کامل"]:
            doc["credit_value"] = amount
            doc["credit_mode"] = "مقطوع"
        elif label == "اجاره":
            doc["rent_value"] = amount
            doc["rent_mode"] = "مقطوع"
        else:
            doc["price_value"] = amount
            doc["price_mode"] = "مقطوع"

    if "رهن" in attr_map:
        doc["credit_value"] = parse_price(attr_map["رهن"])
    if "اجاره" in attr_map:
        doc["rent_value"] = parse_price(attr_map["اجاره"])

    # rent_type
    rent = doc.get("rent_value")
    credit = doc.get("credit_value")
    if rent and credit and rent > 0 and credit > 0:
        doc["rent_type"] = "rent_credit"
    elif rent == 0 and credit and credit > 0:
        doc["rent_type"] = "full_credit"

    doc["rent_credit_transform"] = attr_map.get("قابلیت تبدیل مبلغ رهن و اجاره") == "true"

    # feature
    doc["building_size"] = parse_price(attr_map.get("متراژ")) or parse_price(attr_map.get("مساحت"))
    doc["land_size"] = parse_price(attr_map.get("زمین"))
    doc["rooms_count"] = int(attr_map.get("تعداد اتاق")) if attr_map.get("تعداد اتاق", "").isdigit() else None
    doc["floor"] = int(attr_map.get("طبقه")) if attr_map.get("طبقه", "").isdigit() else None
    doc["construction_year"] = int(attr_map.get("سال ساخت بنا")) if attr_map.get("سال ساخت بنا", "").isdigit() else None
    doc["has_elevator"] = attr_map.get("آسانسور") == "دارد"
    doc["has_parking"] = attr_map.get("پارکینگ") == "دارد"
    doc["has_warehouse"] = attr_map.get("انباری") == "دارد"
    doc["has_balcony"] = attr_map.get("بالکن") == "دارد"
    doc["property_type"] = attr_map.get("نوع ملک")

    # location
    doc["location_latitude"] = str(geo.get("lat")) if geo.get("lat") else None
    doc["location_longitude"] = str(geo.get("lon")) if geo.get("lon") else None
    doc["location_radius"] = 500.0 if geo else None

    # images
    images = []
    for img in item.get("images", []):
        if img.get("url"):
            images.append(img["url"])
    doc["images"] = images

    return doc