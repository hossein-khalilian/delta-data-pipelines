from pymongo import MongoClient
from datetime import datetime
import os 
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "mrestate-dataset"

def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    query = { "title": None }

    result = collection.delete_many(query)
    print(f"Deleted documents: {result.deleted_count}")

if __name__ == "__main__":
    main()
