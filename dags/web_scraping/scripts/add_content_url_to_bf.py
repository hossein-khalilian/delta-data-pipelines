import asyncio
import os
import sys
from pathlib import Path

import redis
import yaml
from dotenv import load_dotenv
from pymongo import MongoClient

from web_scraping.utils.redis_utils import add_to_bloom_filter 

load_dotenv()

CONFIG_PATH = Path(__file__).resolve().parent.parent / "websites.yaml"

with open(CONFIG_PATH, "r") as f:
    yaml_config = yaml.safe_load(f)

site_names = [site["name"] for site in yaml_config["websites"]]

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

REDIS_URL = os.getenv("REDIS_URL")
REDIS_BLOOM_FILTER = os.getenv("REDIS_BLOOM_FILTER")

websites_mongo_collection = [f"{name}-{MONGO_COLLECTION}" for name in site_names]
websites_bloom_filter = [f"{name}_{REDIS_BLOOM_FILTER}" for name in site_names]

BATCH_SIZE = 1000

def ensure_bloom_exists(r, name):
    if r.exists(name) == 0:
        print(f"ERROR: Bloom filter '{name}' does NOT exist in Redis!")
        sys.exit(1)
    print(f"Bloom filter '{name}' exists.")


async def process_collection_and_bloom(collection_name, bloom_name):
    print(f"\nProcessing {collection_name} => {bloom_name} ")

    mongo = MongoClient(MONGO_URI)
    collection = mongo[MONGO_DB][collection_name]

    r = redis.from_url(REDIS_URL)
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
            result = await add_to_bloom_filter(bloom_name, batch)
            inserted += sum(1 for x in result if x == 1)
            duplicates += sum(1 for x in result if x == 0)
            batch = []

    if batch:
        result = await add_to_bloom_filter(bloom_name, batch)
        inserted += sum(1 for x in result if x == 1)
        duplicates += sum(1 for x in result if x == 0)

    print(f"New URLs :  {inserted}")
    print(f"Duplicate :     {duplicates}\n")


async def main():
    for mongo_col, bloom_filter in zip(
        websites_mongo_collection, websites_bloom_filter
    ):
        await process_collection_and_bloom(mongo_col, bloom_filter)


if __name__ == "__main__":
    asyncio.run(main())
