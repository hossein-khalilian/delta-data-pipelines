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

def parse_toman_amount(text: str):
    if not text:
        return None

    text = clean_text(text)
    text = persian_to_english_digits(text)

    # اگر توافقی بود، همون متن برگرده
    if "توافقی" in text:
        return text

    # فقط عدد
    number_match = re.search(r"(\d+)", text)
    if not number_match:
        return text 

    number = int(number_match.group(1))

    if "میلیارد" in text:
        return number * 1_000_000_000
    elif "میلیون" in text:
        return number * 1_000_000
    elif "هزار" in text:
        return number * 1_000
    else:
        return number

def transformer_function(fetched_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transformed_results = []

    feature_map = {
        "طبقه": "floor",
        "تعداد طبقات": "total_floors_count",
        "تعداد اتاق": "rooms_count",
        "تعداد واحد در طبقه": "unit_per_floor",

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
    def normalize_deed_type(value: str):
        if not value:
            return None
        value = clean_text(value)
        value = value.replace("نوع سند", "").replace(":", "").strip()
        if "تک برگ" in value:
            return "تک برگ"
        if "قولنامه" in value:
            return "قولنامه ای"
        return value
    
    # def map_feature(key, value, out):
    #     if key not in feature_map:
    #         return

    #     if isinstance(value, (bool, type(None))):
    #         out[feature_map[key]] = value
    #         return

    #     val = clean_text(str(value))
    #     val = persian_to_english_digits(val)

    #     if "اشاره" in val:
    #         out[feature_map[key]] = None
    #         return

    #     boolean_fields = ["پارکینگ", "آسانسور", "انباری"]

    #     if key in boolean_fields:
    #         if "ندارد" in val:
    #             out[feature_map[key]] = False
    #             return
    #         if "دارد" in val:
    #             out[feature_map[key]] = True
    #             return

    #     out[feature_map[key]] = val
    def map_feature(key, value, out):
        val = clean_text(str(value))
        val = persian_to_english_digits(val)

        # منطق فقط برای آسانسور و انباری
        if key == "آسانسور":
            if "ندارد" in val:
                out["has_elevator"] = False
            elif "آسانسور" in val:
                out["has_elevator"] = True
            else:
                out["has_elevator"] = None
            return

        if key == "انباری":
            if "ندارد" in val:
                out["has_warehouse"] = False
            elif "انباری" in val:
                out["has_warehouse"] = True
            else:
                out["has_warehouse"] = None
            return

        # سایر فیلدها بدون تغییر
        if key in feature_map:
            out[feature_map[key]] = val

    for item in fetched_data:
        html = item.get("html_content")
        url = item.get("content_url")

        if not html or not url:
            print(f"Skipping invalid item: {url}")
            continue

        soup = BeautifulSoup(html, "html.parser")

        try:
            cat1_slug = item.get("listingType")
            # Breadcrumbs
            breadcrumbs_list = [a.get_text(strip=True) for a in soup.select('nav[aria-label="breadcrumb"] a')]
            breadcrumbs = "/".join(breadcrumbs_list) if breadcrumbs_list else None
            city_slug = breadcrumbs_list[2] if len(breadcrumbs_list) > 2 else None
            neighborhood_slug = breadcrumbs_list[4] if len(breadcrumbs_list) > 4 else None
            gallery_imgs = [
                img["src"]
                for img in soup.select("div.relative img")
                if img.get("src") and img["src"].startswith("https://cdn.kilid.com")
            ]

            # Ad code & publish time
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
            price_total = parse_toman_amount(price_tags[0].get_text(strip=True)) if len(price_tags) > 0 else None
            
            # rent & credit 
            rent_credit_blocks = soup.select('div.flex.flex-col.items-end.justify-center span.text-lg.font-bold')

            credit_value = None
            rent_value = None

            if len(rent_credit_blocks) >= 2:
                credit_value_text = clean_text(rent_credit_blocks[0].get_text(strip=True))
                rent_value_text = clean_text(rent_credit_blocks[1].get_text(strip=True))

                credit_value = parse_toman_amount(credit_value_text)

                if re.search(r"رهن\s*کامل", rent_value_text):
                    rent_value = 0
                else:
                    rent_value = parse_toman_amount(rent_value_text)
            # Document type
            doc_div = soup.select_one('div.inline-flex.items-center.bg-gray-50')
            document_type = normalize_deed_type(doc_div.get_text()) if doc_div else None

            # features
            raw_features = {}
            for div in soup.select('div.group.flex.items-center.justify-center'):
                key_span = div.select_one('span.text-nowrap')
                if not key_span:
                    continue

                full_text = clean_text(div.get_text())
                full_text = persian_to_english_digits(full_text)

                # ───── Parking smart detection ─────
                if "پارکینگ" in full_text:

                    # حالت: "اشاره نشده"
                    if "اشاره" in full_text:
                        raw_features["پارکینگ"] = None

                    # حالت: ندارد
                    elif "ندارد" in full_text:
                        raw_features["پارکینگ"] = False

                    # حالت عددی: "1 پارکینگ" / "2 پارکینگ"
                    else:
                        n = re.search(r"(\d+)", full_text)
                        if n:
                            count = int(n.group(1))
                            raw_features["پارکینگ"] = True if count > 0 else False
                        else:
                            raw_features["پارکینگ"] = None

                else:
                    # fallback for other features
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

            user_type = None
            owner_span = soup.find("span", string=re.compile("مالک"))
            if owner_span:
                user_type = "شخصی"
            agency_span = soup.find("span", string=re.compile("آژانس"))
            if agency_span:
                user_type = "مشاور املاک"

            building_size = None
            for span in soup.select('span.text-nowrap'):
                txt = clean_text(span.get_text())
                if re.search(r"\d+\s*متر", txt):
                    num = persian_to_english_digits(re.findall(r"\d+", txt)[0])
                    building_size = int(num) if num.isdigit() else None
                    break

            construction_year = None
            for span in soup.select('span.text-nowrap'):
                txt = clean_text(span.get_text())
                m = re.search(r"ساخت\s*(\d+)", txt)
                if m:
                    num = persian_to_english_digits(m.group(1))
                    construction_year = int(num) if num.isdigit() else None
                    break
                
            is_rebuilt = False
            for btn in soup.select("button span"):
                txt = clean_text(btn.get_text())
                if "بازسازی شده" in txt:
                    is_rebuilt = True
                    break

            price_mode_direct = None
            rent_mode_direct = None

            price_rent_blocks = soup.select(
                'div.flex.w-full.flex-row.items-center.justify-between span.whitespace-nowrap.text-lg.font-bold'
            )

            label_blocks = soup.select(
                'div.flex.w-full.flex-row.items-center.justify-between span.text-sm.font-semibold'
            )

            for label, value in zip(label_blocks, price_rent_blocks):
                label_text = clean_text(label.get_text(strip=True))
                value_text = clean_text(value.get_text(strip=True))

                # BUY
                if cat1_slug == "BUY":
                    if "قیمت" in label_text and "توافقی" in value_text:
                        price_mode_direct = "توافقی"

                # RENT
                if cat1_slug == "RENT":
                    if "رهن و اجاره" in label_text and "توافقی" in value_text:
                        rent_mode_direct = "توافقی"
            # --------- Extract credit_value for RENT from labeled block ---------
            if cat1_slug == "RENT":
                credit_value = None
                credit_labels = soup.select(
                    'div.flex.w-full.flex-row.items-center.justify-between'
                )

                for block in credit_labels:
                    label_el = block.select_one('span.text-sm.font-semibold')
                    value_el = block.select_one('span.text-lg.font-bold')

                    if not label_el or not value_el:
                        continue

                    label_text = clean_text(label_el.get_text())
                    value_text = clean_text(value_el.get_text())

                    # اگر لیبل شامل "رهن" بود
                    if "رهن" in label_text and "تومان" in label_text:
                        credit_value = parse_toman_amount(value_text)
                        break

            out = {
                "created_at": datetime.now(),
                "breadcrumbs": breadcrumbs,
                "cat1_slug" : cat1_slug,
                "cat2_slug": item.get("landuseType"),
                "cat3_slug": item.get("propertyType"),
                "city_slug": city_slug,
                "neighborhood_slug": neighborhood_slug,
                "created_at_month": published_time,

                "user_type": user_type,

                "description": description,
                "title": title,

                "rent_mode": raw_features.get("نوع اجاره"),
                "rent_to_single": raw_features.get("اجاره به مجرد"),
                "rent_type": raw_features.get("نحوه اجاره"),

                "price_mode": raw_features.get("نوع قیمت"),
                "price_value": price_total,
                "credit_value": credit_value,
                "rent_value": rent_value,

                "credit_mode": raw_features.get("نوع رهن"),

                "rent_credit_transform": None,
                "transformable_price": None,
                "transformable_credit": None,
                "transformed_credit": None,
                "transformable_rent": None,
                "transformed_rent": None,

                # defaults for ALL mapped fields (ensures field always exists)
                "land_size": None,
                "building_size": building_size,
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
                "construction_year": construction_year,
                "is_rebuilt": is_rebuilt,
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
                
            # --------- Detect rent_type from HTML ---------
            rent_type_direct = None

            rent_type_blocks = soup.select(
                'div.flex.w-full.flex-row.items-center.justify-between span.text-lg.font-bold.text-primary-900'
            )

            rent_type_labels = soup.select(
                'div.flex.w-full.flex-row.items-center.justify-between span.text-sm.font-semibold'
            )

            for label, value in zip(rent_type_labels, rent_type_blocks):
                label_text = clean_text(label.get_text(strip=True))
                value_text = clean_text(value.get_text(strip=True))

                # اگر آگهی RENT بود و اجاره = رهن کامل
                if cat1_slug == "RENT":
                    # if "اجاره" in label_text and "رهن کامل" in value_text:
                    if "رهن کامل" in value_text:
                        rent_type_direct = "full_credit"

            if cat1_slug == "BUY":
                if price_mode_direct == "توافقی":
                    out["price_mode"] = "توافقی"
                    
                buy_null_fields = [
                    "rent_mode",
                    "rent_to_single",
                    "rent_type",
                    "credit_value",
                    "rent_value",
                    "credit_mode",
                    "rent_credit_transform",
                    "transformable_credit",
                    "transformed_credit",
                    "transformable_rent",
                    "transformed_rent",
                ]
                for field in buy_null_fields:
                    out[field] = None
                
            elif cat1_slug == "RENT":
                out["rent_value"] = rent_value
                out["credit_value"] = credit_value
                out["price_value"] = price_total

                if rent_mode_direct == "توافقی":
                    out["rent_mode"] = "توافقی"
                    
            if cat1_slug == "RENT":
                if rent_type_direct == "full_credit":
                    out["rent_type"] = "full_credit"
                else:
                    try:
                        rv = int(out.get("rent_value")) if out.get("rent_value") is not None else 0
                        cv = int(out.get("credit_value")) if out.get("credit_value") is not None else 0

                        if rv != 0 and cv != 0:
                            out["rent_type"] = "rent_credit"
                    except:
                        pass
                    
                rent_null_fields = [
                    "price_mode",
                    "price_value",
                    "transformable_price",
                ]
                for field in rent_null_fields:
                    out[field] = None
                    
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