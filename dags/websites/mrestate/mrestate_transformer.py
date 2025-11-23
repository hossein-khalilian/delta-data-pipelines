import re
from datetime import datetime


def transformer_function(fetched_data):
    if not fetched_data:
        print("⚠️ No data for transformation.")
        return []

    transformed_data = []
    for item in fetched_data:
        url = item["content_url"]
        raw_json = item["data"]
        try:
            transformed = transform_data(raw_json)
            transformed["content_url"] = url
            transformed_data.append(transformed)
        except Exception as e:
            print(f"Error transforming {url}: {e}")
            continue

    print(f"✅ Transformed {len(transformed_data)} items from mrestate.")
    return transformed_data


def persian_to_english_digits(s: str) -> str:
    persian = "۰۱۲۳۴۵۶۷۸۹"
    english = "0123456789"
    return s.translate(str.maketrans(persian, english))


def transform_data(data: dict) -> dict:
    doc = {}
    inner = data.get("pageProps", {}).get("data", {})

    doc["cat2_slug"] = str(inner.get("category_layer_2")) if inner.get("category_layer_2") is not None else None
    doc["cat3_slug"] = str(inner.get("category_layer_3")) if inner.get("category_layer_3") is not None else None
    doc["city_slug"] = inner.get("city")
    doc["neighborhood_slug"] = inner.get("neighbourhood")
    
    date_publish = inner.get("date_publish")
    if date_publish:
        try:
            doc["created_at_month"] = datetime.fromisoformat(date_publish.replace("Z", "+00:00"))
        except:
            doc["created_at_month"] = None
    else:
        doc["created_at_month"] = None

    real_estate = inner.get("creator_properties", {}).get("real_estate")
    doc["user_type"] = "مشاور املاک" if real_estate else "شخصی"

    doc["description"] = inner.get("more_description") or data.get("pageProps", {}).get("description_tag")
    doc["title"] = inner.get("title") or data.get("pageProps", {}).get("title_tag")

    doc["rent_mode"] = None
    doc["rent_value"] = inner.get("price_rent")
    doc["rent_to_single"] = None
    doc["rent_type"] = None

    doc["price_mode"] = "توافقی" if inner.get("agreed") else ("مقطوع" if inner.get("price_sell") else None)
    doc["price_value"] = inner.get("price_sell")

    doc["credit_mode"] = None
    doc["credit_value"] = inner.get("price_mortgage")
    doc["rent_credit_transform"] = None
    doc["transformable_price"] = None
    doc["transformable_credit"] = None
    doc["transformed_credit"] = None
    doc["transformable_rent"] = None
    doc["transformed_rent"] = None

    doc["land_size"] = None
    doc["building_size"] = inner.get("area")
    doc["deed_type"] = None
    doc["has_business_deed"] = None

    doc["floor"] = None
    doc["rooms_count"] = inner.get("num_bedrooms")
    doc["total_floors_count"] = None
    doc["unit_per_floor"] = None

    more_details = inner.get("more_details", {})
    doc["has_balcony"] = None
    doc["has_elevator"] = more_details.get("elevator")
    doc["has_warehouse"] = more_details.get("storeHouse")
    doc["has_parking"] = bool(more_details.get("parking")) if more_details.get("parking") is not None else None

    doc["construction_year"] = inner.get("year_constructed")
    doc["is_rebuilt"] = None

    doc["has_water"] = None
    doc["has_warm_water_provider"] = None
    doc["has_electricity"] = None
    doc["has_gas"] = None
    doc["has_heating_system"] = None
    doc["has_cooling_system"] = None
    doc["has_restroom"] = None
    doc["has_security_guard"] = more_details.get("janitor")
    doc["has_barbecue"] = None
    doc["building_direction"] = None
    doc["has_pool"] = None
    doc["has_jacuzzi"] = None
    doc["has_sauna"] = None
    doc["floor_material"] = None

    doc["property_type"] = "آپارتمان" if str(inner.get("category_layer_3")) == "3001" else None

    doc["regular_person_capacity"] = None
    doc["extra_person_capacity"] = None
    doc["cost_per_extra_person"] = None
    doc["rent_price_on_regular_days"] = None
    doc["rent_price_on_special_days"] = None
    doc["rent_price_at_weekends"] = None

    doc["location_latitude"] = inner.get("latitude")
    doc["location_longitude"] = inner.get("longitude")
    doc["location_radius"] = None

    # تصاویر
    images = []
    for img in inner.get("list_image", []):
        url = img.get("url")
        if url and url.startswith("/media"):
            url = "https://mrestate.ir" + url
        if url:
            images.append(url)
    doc["images"] = images if images else None

    doc["bread_crumb"] = data.get("pageProps", {}).get("breadcrumb")

    doc["content_url"] = None #

    numeric_fields = [
        "rent_value", "price_value", "credit_value",
        "building_size", "rooms_count", "construction_year",
        "location_latitude", "location_longitude",
    ]

    for field in numeric_fields:
        val = doc.get(field)
        if val is not None:
            val_str = persian_to_english_digits(str(val))
            val_str = re.sub(r"[^\d\.\-]", "", val_str)
            try:
                doc[field] = float(val_str) if "." in val_str or "e" in val_str.lower() else int(val_str)
            except:
                doc[field] = None

    return doc