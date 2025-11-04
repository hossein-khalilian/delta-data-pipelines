import os
from pymongo import MongoClient
from datetime import datetime
from dateutil import parser 
import re

MONGO_URI = os.getenv("MONGO_URI", "mongodb://appuser:appassword@172.16.36.111:27017/delta-datasets")
MONGO_DB = os.getenv("MONGO_DB", "delta-datasets")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "divar-dataset")

LIMIT_DATE = datetime(2025, 11, 4)
RESET_DATE = datetime(2025, 10, 21, 0, 0, 0)

def try_parse_float(value):
    if not isinstance(value, str):
        return value

    value = re.sub(r"[\u200E\u200F]", "", value)

    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    for i, (p, a) in enumerate(zip(persian_digits, arabic_digits)):
        value = value.replace(p, str(i)).replace(a, str(i))

    value = re.sub(r"[,\s٬،]", "", value)

    try:
        num = float(value)

        if num.is_integer():
            return int(num)
        return num
    except ValueError:
        return value


def try_parse_datetime(value):
    if isinstance(value, str):
        try:
            return parser.parse(value)
        except Exception:
            return value
    return value

def normalize_more_than_value(value):
    """
    اگر مقدار به شکل 'بیشتر از X' بود → 'X+'
    این تابع هم برای unit_per_floor و هم برای rooms_count کاربرد دارد.
    """
    if not isinstance(value, str):
        return value

    import re

    # حذف فاصله‌ها و کاراکترهای نامرئی
    value = re.sub(r"[\u200E\u200F\s]", "", value)

    # بررسی وجود عبارت 'بیشتر'
    if "بیشتر" not in value:
        return value

    # تبدیل ارقام فارسی/عربی به انگلیسی
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    for i, (p, a) in enumerate(zip(persian_digits, arabic_digits)):
        value = value.replace(p, str(i)).replace(a, str(i))

    # استخراج عدد از رشته
    match = re.search(r"(\d+)", value)
    if match:
        num = match.group(1)
        return f"{num}+"

    # اگر عددی پیدا نشد، همان مقدار اصلی را برگردان
    return value

def clean_document(doc):
    new_doc = {}

    for key, value in doc.items():

        if key == "_id":
            new_doc[key] = value
            continue
        
        if key == "crawl_timestamp":
            continue

        if key in ["unit_per_floor", "rooms_count"]:
            value = normalize_more_than_value(value)

        if value == "null":
            value = None

        # float
        value = try_parse_float(value)

        if key == "record_timestamp":
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
