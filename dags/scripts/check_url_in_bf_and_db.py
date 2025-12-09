from pymongo import MongoClient
import redis
import os 
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "divar-" + os.getenv("MONGO_COLLECTION")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
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
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        
        exists = r.execute_command("BF.EXISTS", REDIS_BLOOM_FILTER, url)

        return bool(exists)

    except Exception as e:
        print("⚠️ Bloom error:")
        print(e)
        return None

if __name__ == "__main__":
    url = input("enter the URL:").strip()
    
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