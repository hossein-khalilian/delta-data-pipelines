import re
from datetime import datetime


def transform_data(data: dict) -> dict:
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

