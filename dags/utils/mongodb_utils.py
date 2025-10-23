from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils.config import config

def store_to_mongo(**kwargs):
    transformed_data = kwargs["ti"].xcom_pull(
        key="transformed_data", task_ids="transform"
    )
    if not transformed_data:
        print("No data available to store in MongoDB.")
        return

    client = MongoClient(config["mongo_uri"])
    db = client[config["mongo_db"]]
    collection = db[config["mongo_collection"]]
    try:
        collection.create_index("post_token", unique=True)
        for transformed in transformed_data:
            try:
                collection.insert_one(transformed)
                # print(
                #     f"Saved: data for token {transformed['post_token']} in MongoDB"
                # )
            except DuplicateKeyError:
                print(f"Duplicate: token {transformed['post_token']} has already been stored.")
            except Exception as e:
                print(f"Error while saving {transformed['post_token']}: {e}")
    finally:
        client.close()
