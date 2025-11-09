import os
import re
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

BASE_URL = "https://api.divar.ir/v8/posts-v2/web/{token}"

LIMIT_DATE = datetime(2025, 11, 4)
RESET_DATE = datetime(2025, 10, 21, 0, 0, 0)

def persian_to_english_digits(value):
    if not isinstance(value, str):
        return value
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    for i in range(10):
        value = value.replace(persian_digits[i], str(i))
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
        new_key = key
        if key in ["record_timestamp", "created_at"]:
            new_key = "created_at"
            value = try_parse_datetime(value)
        if new_key == "created_at_month":
            if isinstance(value, str):
                m = re.match(r"^(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})$", value)
                if m:
                    value = f"{m.group(1)} {m.group(2)}"
            value = try_parse_datetime(value)
            if isinstance(value, datetime) and value > LIMIT_DATE:
                value = RESET_DATE
        new_doc[new_key] = value
    return new_doc

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

indexes = collection.index_information()
for index_name, index_data in indexes.items():
    if "post_token" in index_data["key"][0]:
        print(f"🗑 Removing old index: {index_name}")
        collection.drop_index(index_name)

print("Creating unique index on content_url ...")
collection.create_index("content_url", unique=True, sparse=True)

batch_size = 1000
operations = []
updated = 0
skipped = 0
errors = 0

cursor = collection.find({}, no_cursor_timeout=True)
for doc in cursor:
    # اگر content_url از قبل موجود است، رد می‌کنیم
    if "content_url" in doc and doc["content_url"]:
        skipped += 1
        continue

    token = doc.get("post_token")
    if not token:
        skipped += 1
        continue

    new_content_url = BASE_URL.format(token=token)
    
    # بررسی وجود URL در دیتابیس
    if collection.find_one({"content_url": new_content_url}):
        skipped += 1
        continue

    operations.append(
        UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"content_url": new_content_url}, "$unset": {"post_token": ""}}
        )
    )

    if len(operations) == batch_size:
        result = collection.bulk_write(operations, ordered=False)
        updated += result.modified_count
        operations = []



# Write remaining operations
if operations:
    result = collection.bulk_write(operations, ordered=False)
    updated += result.modified_count

cursor.close()
print(f"✅ Updated: {updated}")
print(f"⚠️ Skipped (duplicates or empty): {skipped}")
print(f"❌ Errors: {errors}")

cursor = collection.find({}, no_cursor_timeout=True)
cleaned_count = 0
try:
    for doc in cursor:
        cleaned = clean_document(doc)
        if cleaned != doc:
            collection.replace_one({"_id": doc["_id"]}, cleaned)
            cleaned_count += 1
            if cleaned_count % 100 == 0:
                print(f"{cleaned_count} documents cleaned and updated...")
finally:
    cursor.close()
    client.close()

print(f"✅ All done! {cleaned_count} documents cleaned and updated.")
