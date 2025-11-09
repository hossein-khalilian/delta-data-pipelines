import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
print(MONGO_URI)
print(MONGO_DB)
print(MONGO_COLLECTION)

BASE_URL = "https://api.divar.ir/v8/posts-v2/web/{token}"

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

print("✅ Connected to MongoDB")

indexes = collection.index_information()
for index_name, index_data in indexes.items():
    if "post_token" in index_data["key"][0]:  # detects ("post_token", 1)
        print(f"🗑 Removing old index: {index_name}")
        collection.drop_index(index_name)

print(" Creating unique index on content_url ...")
collection.create_index("content_url", unique=True, sparse=True)

updated = 0
skipped = 0
errors = 0

cursor = collection.find({"post_token": {"exists": True}}, no_cursor_timeout=True)

for doc in cursor:
    token = doc.get("post_token")
    if not token:
        skipped += 1
        continue

    new_content_url = BASE_URL.format(token=token)

    try:
        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {"content_url": new_content_url},
                "$unset": {"post_token": ""},  # del post_token
            },
        )
        updated += 1
    except DuplicateKeyError:
        print(f"⚠️ Duplicate content_url detected, skipping: {new_content_url}")
        skipped += 1
    except Exception as e:
        print(f"❌ Error updating {token}: {e}")
        errors += 1

cursor.close()
client.close()

print("----- Migration Completed -----")
print(f"✅ Updated: {updated}")
print(f"⚠️ Skipped (duplicates or empty): {skipped}")
print(f"❌ Errors: {errors}")
