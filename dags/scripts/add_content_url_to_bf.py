from pymongo import MongoClient
from redisbloom.client import Client as RedisBloom
import redis
import sys
from dotenv import load_dotenv
import os

load_dotenv()

websites_mongo_colletion = ["divar-dataset_1","mrestate-dataset_1","sheypoor-dataset_1"]

websites_bloom_filter = ["divar_urls_1","mrestate_urls_1","sheypoor_urls_1"]

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

BATCH_SIZE = 1000

def ensure_bloom_exists(r, name):
    if r.exists(name) == 0:
        print(f"ERROR: Bloom filter '{name}' does NOT exist in Redis!")
        sys.exit(1)
    print(f"Bloom filter '{name}' exists.")


def process_collection_and_bloom(collection_name, bloom_name):
    print(f"\n Processing {collection_name} => {bloom_name} ")

    mongo = MongoClient(MONGO_URI)
    collection = mongo[MONGO_DB][collection_name]

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    rb = RedisBloom(host=REDIS_HOST, port=REDIS_PORT)

    ensure_bloom_exists(r, bloom_name)

    cursor = collection.find(
        {"content_url": {"$exists": True, "$ne": None}},
        {"content_url": 1, "_id": 0},
    )

    batch = []
    inserted = 0
    duplicates = 0

    for doc in cursor:
        url = doc.get("content_url")
        if not url:
            continue

        batch.append(url)

        if len(batch) >= BATCH_SIZE:
            result = rb.bfMAdd(bloom_name, *batch)
            inserted += sum(result)
            duplicates += len(result) - sum(result)
            batch = []

    if batch:
        result = rb.bfMAdd(bloom_name, *batch)
        inserted += sum(result)
        duplicates += len(result) - sum(result)

    print(f"New URLs inserted:  {inserted}")
    print(f"Duplicate URLs:     {duplicates}\n")


def main():
    for mongo_col, bloom_filter in zip(websites_mongo_colletion, websites_bloom_filter):
        process_collection_and_bloom(mongo_col, bloom_filter)

if __name__ == "__main__":
    main()
