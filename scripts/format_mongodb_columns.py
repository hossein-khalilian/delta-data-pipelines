import os
from pymongo import MongoClient
from datetime import datetime
from dateutil import parser 
import re
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

LIMIT_DATE = datetime(2025, 11, 4)
RESET_DATE = datetime(2025, 10, 21, 0, 0, 0)

def persian_to_english_digits(value):
    if not isinstance(value, str):
        return value
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    for i in range(10):
        value = value.replace(persian_digits[i], str(i)).replace(arabic_digits[i], str(i))
    return value

def try_parse_float(value):
    if not isinstance(value, str):
        return value

    value = persian_to_english_digits(value)
    value = re.sub(r"[\u200E\u200F,\s٬،]", "", value)

    try:
        num = float(value)
        return int(num) if num.is_integer() else num
    except ValueError:
        return value
    
def try_parse_datetime(value):
    if isinstance(value, str):
        value = persian_to_english_digits(value).strip()

        # اگر بین تاریخ و ساعت فاصله وجود نداشت مثل: 2025-10-2114:20:31
        # اضافه کردن فاصله قبل از ساعت
        m = re.match(r"^(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})$", value)
        if m:
            value = f"{m.group(1)} {m.group(2)}"

        try:
            return parser.parse(value)
        except Exception:
            return value
    return value

def normalize_more_than_value(value):
    if not isinstance(value, str):
        return value

    value = persian_to_english_digits(value)
    value = re.sub(r"[\u200E\u200F\s]", "", value)

    if "بیشتراز" in value or "بیشتر" in value:
        match = re.search(r"(\d+)", value)
        if match:
            return f"{match.group(1)}+"
    return value

def normalize_construction_year(value):
    if not isinstance(value, str):
        return value
    
    cleaned = persian_to_english_digits(value).replace(" ", "")

    if "قبل" in cleaned and "1370" in cleaned:
        return -1370
    
    return try_parse_float(cleaned)

def clean_document(doc):
    new_doc = {}

    for key, value in doc.items():

        if key == "_id":
            new_doc[key] = value
            continue
        
        if key == "crawl_timestamp":
            continue

        if value == "null":
            value = None

        if key == "rooms_count":
            if isinstance(value, str) and "بدون" in value:
                value = 0
            else:
                value = normalize_more_than_value(value)

        if key == "unit_per_floor":
            value = normalize_more_than_value(value)

        if key == "construction_year":
            value = normalize_construction_year(value)

        value = persian_to_english_digits(value)

        value = try_parse_float(value)

        if key in ["record_timestamp", "created_at"]:
            new_key = "created_at"
            value = try_parse_datetime(value)
        else:
            new_key = key

        if new_key == "created_at_month":
            value = try_parse_datetime(value)
            if isinstance(value, datetime) and value > LIMIT_DATE:
                value = RESET_DATE

        new_doc[new_key] = value

    return new_doc

def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    cursor = col.find({}, no_cursor_timeout=True)
    updated = 0

    try:
        for doc in cursor:
            cleaned = clean_document(doc)
            if cleaned != doc:
                col.replace_one({"_id": doc["_id"]}, cleaned)
                updated += 1
                if updated % 100 == 0:
                    print(f"{updated} documents updated...")
    finally:
        cursor.close()
        client.close()

    print(f"✅ Done! {updated} documents cleaned and updated.")

if __name__ == "__main__":
    main()
