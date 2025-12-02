from pymongo import MongoClient
from redisbloom.client import Client as RedisBloom
import redis
import sys

# Configs
MONGO_URI = "mongodb://appuser:appassword@172.16.36.111:27017/delta-datasets"
MONGO_DB = "delta-datasets"
MONGO_COLLECTION = "mrestate-dataset_1"

REDIS_HOST = "172.16.36.111"
REDIS_PORT = 6379
REDIS_BLOOM_FILTER = "mrestate_urls_1"

BATCH_SIZE = 1000  

def ensure_bloom_exists(r, name):
    if r.exists(name) == 0:
        print(f"ERROR: Bloom filter '{name}' does NOT exist in Redis!")
        sys.exit(1)
    print(f"Bloom filter '{name}' exists. Continuing...")


def main():
    mongo = MongoClient(MONGO_URI)
    collection = mongo[MONGO_DB][MONGO_COLLECTION]

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    rb = RedisBloom(host=REDIS_HOST, port=REDIS_PORT)

    ensure_bloom_exists(r, REDIS_BLOOM_FILTER)

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
            result = rb.bfMAdd(REDIS_BLOOM_FILTER, *batch)
            inserted += sum(result)
            duplicates += len(result) - sum(result)
            batch = []

    if batch:
        result = rb.bfMAdd(REDIS_BLOOM_FILTER, *batch)
        inserted += sum(result)
        duplicates += len(result) - sum(result)
        
    print(f"New URLs inserted:  {inserted}")
    print(f"Duplicate URLs:     {duplicates}")


if __name__ == "__main__":
    main()
