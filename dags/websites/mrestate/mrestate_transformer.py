import re
from datetime import datetime, timezone

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
    more_details = data.get("pageProps", {}).get("data", {}).get("data", {}).get("more_details", {})

    doc["created_at"] = datetime.now()
    
    breadcrumb = data.get("pageProps", {}).get("data", {}).get("breadcrumb", [])
    cat2_slug = None
    cat3_slug = None
    cat2_candidates = ["خرید", "اجاره"]
    cat3_candidates = [
        "آپارتمان", "برج", "پنت هاوس", "کلنگی", "مستغلات", "زمین",
        "سوییت", "ویلا", "آپارتمان اداری", "سند اداری",
        "مغازه", "کارخانه", "کارگاه", "انبار", "سوله"
    ]

    if len(breadcrumb) >= 2:
        name = breadcrumb[1].get("name") or ""

        first_word = name.split(" ")[0].strip()
        if first_word in cat2_candidates:
            cat2_slug = first_word
            
        name_without_cat2 = name.replace(first_word, "", 1).strip()

        for item in cat3_candidates:
            if name_without_cat2.startswith(item):
                cat3_slug = item
                break

    doc["cat2_slug"] = cat2_slug
    doc["cat3_slug"] = cat3_slug

    doc["city_slug"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("city")
    doc["neighborhood_slug"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("neighbourhood")
    
    date_str = data.get("pageProps", {}).get("data", {}).get("data", {}).get("date_publish")

    if date_str:
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(date_str)
        dt = dt.astimezone(timezone.utc) 
        doc["created_at_month"] = dt     
    else:
        doc["created_at_month"] = None
            
    creator = data.get("pageProps", {}).get("data", {}).get("data", {}).get("creator_properties", {})
    is_owner = data.get("pageProps", {}).get("data", {}).get("data", {}).get("is_owner", False)

    if is_owner:
        doc["user_type"] = "شخصی"
    elif creator.get("real_estate") is not None:
        doc["user_type"] = "مشاور املاک"
    elif creator.get("consultant") is not None:
        doc["user_type"] = "مشاور مستقل"
    else:
        doc["user_type"] = None

    doc["description"] = (
        data.get("pageProps", {}).get("data", {}).get("data", {}).get("more_description")
    )
    doc["title"] = (
        data.get("pageProps", {}).get("data", {}).get("data", {}).get("title")
    )    
    

    doc["rent_mode"] = None
    doc["rent_value"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("price_rent")
    doc["rent_to_single"] = None
    
    price_rent = data.get("pageProps", {}).get("data", {}).get("data", {}).get("price_rent")
    price_sell = data.get("pageProps", {}).get("data", {}).get("data", {}).get("price_sell")
    price_mortgage = data.get("pageProps", {}).get("data", {}).get("data", {}).get("price_mortgage")


    if price_mortgage not in (None, 0) and (price_rent is None or price_rent == 0):
        doc["rent_type"] = "full_credit"
    elif price_mortgage not in (None, 0) and price_rent not in (None, 0):
        doc["rent_type"] = "rent_credit"
        
    doc["price_mode"] = None
    doc["price_value"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("price_sell")
    doc["credit_mode"] = None
    doc["credit_value"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("price_mortgage")
    
    if price_rent is None and price_sell is None and price_mortgage is None:
        if doc.get("cat2_slug") == "اجاره":
            doc["rent_mode"] = "توافقی"
            doc["credit_mode"] = "توافقی"
        elif doc.get("cat2_slug") == "خرید":
            doc["price_mode"] = "توافقی"
        
    doc["rent_credit_transform"] = None
    doc["transformable_price"] = None
    doc["transformable_credit"] = None
    doc["transformed_credit"] = None
    doc["transformable_rent"] = None
    doc["transformed_rent"] = None
    
    more_details = data.get("pageProps", {}).get("data", {}).get("data", {}).get("more_details", {})

    doc["land_size"] = None
    doc["building_size"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("area")
    doc["deed_type"] = None
    doc["has_business_deed"] = None

    doc["floor"] =  more_details.get("floor")
    doc["rooms_count"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("num_bedrooms")
    doc["total_floors_count"] = None
    doc["unit_per_floor"] = None

    doc["has_balcony"] = more_details.get("balcony")
    doc["has_elevator"] = more_details.get("elevator")
    doc["has_warehouse"] = more_details.get("storeHouse")
    parking_value = more_details.get("parking")
    doc["has_parking"] = (isinstance(parking_value, (int, float)) and parking_value > 0)

    doc["construction_year"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("year_constructed")
    doc["is_rebuilt"] = None

    doc["has_water"] = None
    doc["has_warm_water_provider"] = None
    doc["has_electricity"] = None
    doc["has_gas"] = None
    doc["has_heating_system"] = None
    doc["has_cooling_system"] = None
    doc["has_restroom"] = None
    doc["has_security_guard"] = more_details.get("security")
    doc["has_barbecue"] = None
    doc["building_direction"] = None
    doc["has_pool"] = more_details.get("pool")
    doc["has_jacuzzi"] = more_details.get("jacuzzi")
    doc["has_sauna"] = more_details.get("sauna")
    doc["floor_material"] = None

    doc["property_type"] = None

    doc["regular_person_capacity"] = None
    doc["extra_person_capacity"] = None
    doc["cost_per_extra_person"] = None
    doc["rent_price_on_regular_days"] = None
    doc["rent_price_on_special_days"] = None
    doc["rent_price_at_weekends"] = None

    doc["location_latitude"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("latitude")
    doc["location_longitude"] = data.get("pageProps", {}).get("data", {}).get("data", {}).get("longitude")
    doc["location_radius"] = None

    images = []
    for img in data.get("pageProps", {}).get("data", {}).get("data", {}).get("list_image", []):
        url = img.get("url")
        if url:
            if url.startswith("/media"):
                url = "https://mrestate.ir" + url
            images.append(url)

    doc["images"] = images if images else None


    breadcrumb_list = data.get("pageProps", {}).get("data", {}).get("breadcrumb", [])
    if breadcrumb_list:
        doc["bread_crumb"] = "\n/\n".join([b.get("name") for b in breadcrumb_list if b.get("name")]) + "\n/"
    else:
        doc["bread_crumb"] = None

    doc["content_url"] = None 

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