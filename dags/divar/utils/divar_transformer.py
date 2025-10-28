import re
from datetime import datetime


def transform_data(data: dict) -> dict:
    doc = {}
    doc["record_timestamp"] = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    doc["cat2_slug"] = data.get("analytics", {}).get("cat2") or None
    doc["cat3_slug"] = data.get("analytics", {}).get("cat3") or None
    city_data = data.get("city")
    if isinstance(city_data, dict):
        doc["city_slug"] = city_data.get("second_slug", None)
    else:
        doc["city_slug"] = city_data or None
    doc["neighborhood_slug"] = data.get("webengage", {}).get("district") or None
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
        data.get("seo", {}).get("post_seo_schema", {}).get("description") or None
    )
    doc["title"] = data.get("seo", {}).get("web_info", {}).get("title") or None
    
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
        value = price_widget.get("data", {}).get("value", None)
        doc["price_value"] = value.replace(" تومان", "") if value != None else None
        
    rent_slider = next((w for w in widgets if w.get("widget_type") == "RENT_SLIDER"), None)
    
    unexpandable_rows = {
        w.get("data", {}).get("title"): w.get("data", {}).get("value")
        for w in widgets if w.get("widget_type") == "UNEXPANDABLE_ROW"
    }

    credit_row_value = unexpandable_rows.get("ودیعه")
    rent_row_value = unexpandable_rows.get("اجارهٔ ماهانه")
    rent_credit_row_value = unexpandable_rows.get("ودیعه و اجاره")

    breadcrumb_section = next((s for s in data.get("sections", []) if s.get("section_name") == "BREADCRUMB"), {})
    breadcrumb_widget = next((w for w in breadcrumb_section.get("widgets", []) if w.get("widget_type") == "BREADCRUMB"), None)
    current_page_title = breadcrumb_widget.get("data", {}).get("current_page_title", "") if breadcrumb_widget else ""

    web_credit = data.get("webengage", {}).get("credit", 0)
    web_rent = data.get("webengage", {}).get("rent", 0)
    
    if rent_slider:
        rent_data = rent_slider.get("data", {}) or {}
        credit = rent_data.get("credit", {}) or {}
        rent = rent_data.get("rent", {}) or {}

        raw_credit = credit.get("value")
        raw_rent = rent.get("value")
        trans_credit = credit.get("transformed_value")
        trans_rent = rent.get("transformed_value")

        doc["credit_value"] = float(raw_credit) if raw_credit else None
        doc["rent_value"] = float(raw_rent) if raw_rent else None
        doc["transformed_credit"] = float(trans_credit) if trans_credit else None
        doc["transformed_rent"] = float(trans_rent) if trans_rent else None

        doc["transformable_credit"] = bool(trans_credit)
        doc["transformable_rent"] = bool(trans_rent)
        doc["rent_credit_transform"] = bool(trans_credit and trans_rent)

        doc["transformable_price"] = bool(trans_credit and trans_rent)

        # rent_type
        if doc["rent_credit_transform"]:
            doc["rent_type"] = "rent_credit"
        elif has_credit and not has_rent:
            doc["rent_type"] = "full_credit"
        else:
            doc["rent_type"] = None

        # rent_mode
        if not has_credit and not has_rent:
            doc["rent_mode"] = "مجانی"
        elif doc["rent_credit_transform"]:
            doc["rent_mode"] = "توافقی"
        elif rent_credit_row_value == "غیر قابل تبدیل":
            doc["rent_mode"] = "مقطوع"
        else:
            doc["rent_mode"] = "توافقی" 
        # credit_mode
        doc["credit_mode"] = "مقطوع" if has_credit and not doc["transformable_credit"] else None

    else:
        # fallback به UNEXPANDABLE_ROW
        if credit_row_value:
            cleaned = credit_row_value.replace("‏", "").replace("،", "").replace(" تومان", "").strip()
            doc["credit_value"] = float(cleaned) if cleaned.replace(".", "").isdigit() else None

        if rent_row_value and "رایگان" in rent_row_value:
            doc["rent_value"] = 0
        elif rent_row_value:
            cleaned = rent_row_value.replace("‏", "").replace("،", "").replace(" تومان", "").strip()
            doc["rent_value"] = float(cleaned) if cleaned.replace(".", "").isdigit() else None

        # fallback به webengage
        if doc["credit_value"] is None:
            doc["credit_value"] = float(web_credit) if web_credit else None
        if doc["rent_value"] is None:
            doc["rent_value"] = float(web_rent) if web_rent else None

        has_credit = doc["credit_value"] is not None and doc["credit_value"] > 0
        has_rent = doc["rent_value"] is not None and doc["rent_value"] > 0

        doc["rent_type"] = "full_credit" if has_credit and not has_rent else "full_rent" if has_rent and not has_credit else "rent_credit"
        doc["credit_mode"] = "مقطوع" if has_credit else None
        
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
        doc[field] = None
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
            doc["land_size"] = widget.get("data", {}).get("value", None)
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
            if doc["building_size"] != None:
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
                deed_type_map.get(raw_deed_type, None) if raw_deed_type else None
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
            deed_type_map.get(raw_deed_type, None) if raw_deed_type else None
        )
    doc["has_business_deed"] = None
    floor_map = {"همکف": "0", "هم‌کف": "0"}
    for widget in widgets:
        if (
            widget.get("widget_type") == "UNEXPANDABLE_ROW"
            and widget.get("data", {}).get("title") == "طبقه"
        ):
            raw_floor = widget.get("data", {}).get("value", None)
            if raw_floor != None:
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
                            doc["floor"] = None
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
            if doc["rooms_count"] != None:
                break
    for widget in widgets:
        if (
            widget.get("widget_type") == "UNEXPANDABLE_ROW"
            and widget.get("data", {}).get("title") == "طبقه"
        ):
            floor_value = widget.get("data", {}).get("value", None)
            if floor_value != None:
                match = re.search(r"(\d+)\s*از\s*(\d+)", floor_value)
                if match:
                    doc["total_floors_count"] = match.group(2)
                    break
    if doc["total_floors_count"] == None and description:
        match = re.search(r"(\d+)\s*از\s*(\d+)", description)
        if match:
            doc["total_floors_count"] = match.group(2)
    doc["unit_per_floor"] = next(
        (
            m.get("data", {}).get("value")
            for m in modal_features
            if m.get("data", {}).get("title") == "تعداد واحد در طبقه"
        ),
        None,
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
        doc[f] = None
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
            doc["is_rebuilt"] = mdata.get("value", None) == "بازسازی شده"
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
                            doc["construction_year"] = item.get("value", None)
                if widget.get("widget_type") == "UNEXPANDABLE_ROW":
                    mdata = widget.get("data", {}) or {}
                    title = mdata.get("title", "") or ""
                    if title == "نوع ملک":
                        doc["property_type"] = property_type_map.get(
                            mdata.get("value", ""), "other"
                        )
    doc["regular_person_capacity"] = None
    doc["extra_person_capacity"] = None
    doc["cost_per_extra_person"] = None
    doc["rent_price_on_regular_days"] = None
    doc["rent_price_on_special_days"] = None
    doc["rent_price_at_weekends"] = None
    lat = None
    lon = None
    radius = None
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
                radius = fuzzy.get("radius") or fuzzy.get("r") or None
            elif exact:
                lat = exact.get("latitude") or exact.get("lat") or lat
                lon = exact.get("longitude") or exact.get("lng") or lon
                radius = None
            else:
                radius = location.get("radius", None)
    doc["location_latitude"] = str(lat) if lat is not None else None
    doc["location_longitude"] = str(lon) if lon is not None else None
    doc["location_radius"] = radius if radius is not None else None
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

