import os

import redis
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "divar-" + os.getenv("MONGO_COLLECTION")

REDIS_URL = os.getenv("REDIS_URL")
REDIS_BLOOM_FILTER = "divar_" + os.getenv("REDIS_BLOOM_FILTER")


def check_url_in_mongo(url: str):
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        result = collection.find_one({"content_url": url})
        return bool(result)

    except Exception as e:
        print("⚠️ Error while connecting to MongoDB or searching:")
        print(e)
        return None


def check_url_in_bloom(url: str):
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)

        exists = r.execute_command("BF.EXISTS", REDIS_BLOOM_FILTER, url)

        return bool(exists)

    except Exception as e:
        print("⚠️ Bloom error:")
        print(e)
        return None


if __name__ == "__main__":
    token = input("enter the token:").strip()

    url = f"https://api.divar.ir/v8/posts-v2/web/{token}"

    # MongoDB check
    mongo_result = check_url_in_mongo(url)
    if mongo_result is True:
        print("MongoDB: ✅ URL exists")
    elif mongo_result is False:
        print("MongoDB: ❌ URL not found")

    # Bloom Filter check
    bloom_result = check_url_in_bloom(url)
    if bloom_result is True:
        print("Bloom Filter: ✅ Duplicate detected")
    elif bloom_result is False:
        print("Bloom Filter: ❌ Not a duplicate")

