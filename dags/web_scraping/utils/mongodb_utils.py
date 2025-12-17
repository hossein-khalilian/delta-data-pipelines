from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from utils.config import config

def store_to_mongo(transformed_data, collection_name=None):

    if not transformed_data:
        print("No data available to store in MongoDB.")
        return 0

    if not collection_name:
        raise ValueError("❌ Error: collection_name must be provided ")

    client = MongoClient(config["mongo_uri"])
    db = client[config["mongo_db"]]
    collection = db[collection_name]

    saved_count = 0
    
    try:
        collection.create_index("content_url", unique=True, sparse=True)
        for transformed in transformed_data:
            
            if "content_url" not in transformed or not transformed["content_url"]:
                print("⚠️ Skipping record without content_url")
                continue
            
            try:
                collection.insert_one(transformed)
                saved_count += 1 
                # print(
                #     f"Saved: data for token {transformed['post_token']} in MongoDB"
                # )
            except DuplicateKeyError:
                print(f"Duplicate: content_url {transformed['content_url']} has already been stored.")
            except Exception as e:
                print(f"Error while saving {transformed.get('content_url')}: {e}")
                
        print(f"✅ Saved {saved_count} new records in MongoDB")
        
        return saved_count 
    
    finally:
        client.close()