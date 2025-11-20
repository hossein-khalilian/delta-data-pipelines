import re
from datetime import datetime, timedelta

def persian_to_english_digits(s):
    """Convert Persian digits to English digits"""
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    english_digits = "0123456789"
    return s.translate(str.maketrans(persian_digits, english_digits))

def text_to_date(text):
    text = text.strip()
    text = persian_to_english_digits(text)
    now = datetime.now()

    if not re.search(r"\d+", text):
        if "لحظاتی پیش" in text:
            return now
        if "ساعاتی پیش" in text:
            return now - timedelta(hours=2)
        if "دقایقی پیش" in text:
            return now - timedelta(minutes=5)
        #if not
        return now

    # if has number
    match = re.search(r"(\d+)", text)
    amount = int(match.group(1))

    if "روز" in text:
        return now - timedelta(days=amount)
    elif "هفته" in text:
        return now - timedelta(weeks=amount)
    elif "ماه" in text:
        return now - timedelta(days=amount * 30)

    return now


def extract_publish_time(data):
    """Extract the ad posting time"""
    publish_time = None

    # TITLE
    title_section = next(
        (section for section in data.get("sections", []) if section.get("section_name") == "TITLE"),
        None
    )

    if title_section:
        # LEGEND_TITLE_ROW 
        legend_widget = next(
            (w for w in title_section.get("widgets", [])
             if w.get("widget_type") == "LEGEND_TITLE_ROW"),
            None
        )

        if legend_widget:
            subtitle = legend_widget.get("data", {}).get("subtitle")
            if subtitle:
                # before the word 'در' 
                time_part = subtitle.split(" در ")[0].strip()
                try:
                    dt = text_to_date(time_part)
                    # publish_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                    publish_time = dt
                except Exception:
                    publish_time = None

    return publish_time


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

def transform_data(item: dict) -> dict:

    # item = raw_data.get("data", {}) if "data" in raw_data else raw_data  
    
    attributes = item.get("attributes", {})
    full_attrs = item.get("fullAttributes", [])
    geo = item.get("geo", {})

    # Helpers
    def get_attr(key):
        for a in full_attrs:
            if a.get("key") == key:
                return a.get("value")
        return None

    def price_clean(v):
        if not v:
            return None
        v = re.sub(r"[^\d]", "", str(v))
        return float(v) if v else None

    # breadcrumb
    cats = attributes.get("categories", [])
    if len(cats) > 0:
        b1 = cats[0].get("name")
    else:
        b1 = None
    if len(cats) > 1:
        b2 = cats[1].get("name")
    else:
        b2 = None

    bread_crumb = None
    if b1:
        bread_crumb = b1
        if b2:
            bread_crumb += "/" + b2

    # location
    loc = attributes.get("location")
    if loc:
        parts = [x.strip() for x in loc.split("،")]
        city_slug = to_slug(parts[0]) if len(parts) > 0 else None
        neighborhood_slug = to_slug(parts[1]) if len(parts) > 1 else None
    else:
        city_slug = None
        neighborhood_slug = None

    #creat_at_month
    publish_time = None
    if "timePassedLabel" in attributes:
        try:
            publish_time = text_to_date(attributes.get("timePassedLabel"))
        except Exception:
            publish_time = datetime.now()

    created_at_month = publish_time if publish_time else datetime.now()
    
    # Price fields
    price_mode = None
    price_value = None
    rent_mode = None
    rent_value = None
    credit_mode = None
    credit_value = None

    pr_list = attributes.get("price", [])
    if len(pr_list) > 0:
        p = pr_list[0]
        label = p.get("label", "").strip()
        amount = price_clean(p.get("amount"))

        if label in ["رهن", "رهن کامل"]:
            credit_value = amount
            credit_mode = "مقطوع"
        elif label == "اجاره":
            rent_value = amount
            rent_mode = "مقطوع"
        else:
            price_value = amount
            price_mode = "مقطوع"

    # building size
    building_size = price_clean(get_attr("متراژ"))

    # deed
    deed_type = get_attr("نوع سند")

    # floor
    f = get_attr("طبقه ملک")
    floor = int(f) if f and f.isdigit() else None

    # rooms
    r = get_attr("تعداد اتاق")
    rooms_count = int(r) if r and r.isdigit() else None

    # unit per floor
    u = get_attr("تعداد واحد در طبقه")
    unit_per_floor = int(u) if u and u.isdigit() else None

    # Facilities
    has_elevator = True if get_attr("آسانسور") == "دارد" else None
    has_warehouse = True if get_attr("انباری") == "دارد" else None
    has_parking = True if get_attr("پارکینگ") == "دارد" else None

    # Construction year
    y = get_attr("سال ساخت بنا")
    construction_year = int(y) if y and y.isdigit() else None

    # price type logic
    if credit_value and (not rent_value or rent_value == 0):
        rent_type = "full_credit"
    elif credit_value and rent_value:
        rent_type = "rent_credit"
    else:
        rent_type = None

    rent_credit_transform = True if get_attr("قابلیت تبدیل مبلغ رهن و اجاره") == "true" else None

    # geo
    location_latitude = geo.get("lat") if geo else None
    location_longitude = geo.get("lon") if geo else None

    #image
    image = attributes.get("images", {}).get("thumbnails", {}).get("round")

    # content_url
    content_url = attributes.get("url")

    # FINAL REQUIRED COLUMN ORDER
    return {
        "created_at": datetime.now(),
        "cat2_slug": to_slug(b1) if b1 else None,
        "cat3_slug": to_slug(b2) if b2 else None,
        "city_slug": city_slug,
        "neighborhood_slug": neighborhood_slug,
        "created_at_month": created_at_month,
        "user_type": None,
        "description": item.get("description") or None,
        "title": attributes.get("title") or None,
        "rent_mode": rent_mode,
        "rent_value": rent_value,
        "rent_to_single": None,
        "rent_type": rent_type,
        "price_mode": price_mode,
        "price_value": price_value,
        "credit_mode": credit_mode,
        "credit_value": credit_value,
        "rent_credit_transform": rent_credit_transform,
        "transformable_price": None,
        "transformable_credit": None,
        "transformed_credit": None,
        "transformable_rent": None,
        "transformed_rent": None,
        "land_size": None,
        "building_size": building_size,
        "deed_type": deed_type,
        "has_business_deed": True if deed_type == "تجاری" else None,
        "floor": floor,
        "rooms_count": rooms_count,
        "total_floors_count": None,
        "unit_per_floor": unit_per_floor,
        "has_balcony": None,
        "has_elevator": has_elevator,
        "has_warehouse": has_warehouse,
        "has_parking": has_parking,
        "construction_year": construction_year,
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
        "property_type": get_attr("نوع ملک") or None,
        "regular_person_capacity": None,
        "extra_person_capacity": None,
        "cost_per_extra_person": None,
        "rent_price_on_regular_days": None,
        "rent_price_on_special_days": None,
        "rent_price_at_weekends": None,
        "location_latitude": location_latitude,
        "location_longitude": location_longitude,
        "location_radius": None,
        "image": image,
        "bread_crumb": bread_crumb,
        "content_url": content_url,
    }