import os
import subprocess
from pymongo import MongoClient
# from utils.config import config
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
# MONGO_COLLECTION_ENV = config["mongo_collection"]
# MONGO_COLLECTION = f'divar-{MONGO_COLLECTION_ENV}'
MONGO_COLLECTION = "divar-dataset"

CSV_NAME = "filtered_data.csv"

TMP_DIR = "/home/Kowsar/DELTA/filtered_data"
LOCAL_CSV = f"{TMP_DIR}/{CSV_NAME}"

EXCLUDED_FIELDS = [
    '_id', 'created_at', 'content_url', 'images',
    'location_radius', 'credit_value', 'has_security_guard', 'has_barbecue',
    'has_pool', 'has_jacuzzi', 'has_business_deed', 'has_sauna',
    'transformed_rent', 'transformable_rent',
    'transformable_credit', 'transformable_price', 'rent_credit_transform',
    'transformed_credit', 'credit_mode', 'rent_mode',
    'rent_price_at_weekends', 'rent_price_on_special_days',
    'cost_per_extra_person', 'extra_person_capacity',
    'regular_person_capacity', 'rent_price_on_regular_days', 'rent_value',
    'rent_to_single', 'property_type', 'has_electricity', 'price_mode',
    'has_gas', 'cat2_slug', 'description'
]

def extract_function():
    os.makedirs(TMP_DIR, exist_ok=True)

    try:
        print(f"TMP_DIR ensured: {TMP_DIR}")
        
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        sample_doc = collection.find_one()
        if not sample_doc:
            raise Exception("Mongo collection is empty")
        
        fields = [key for key in sample_doc.keys() if key not in EXCLUDED_FIELDS]
        
        print(f"Found {len(fields)} fields after excluding {len(EXCLUDED_FIELDS)} fields")
        
        fields_file = f"{TMP_DIR}/fields.txt"
        with open(fields_file, 'w') as f:
            for field in fields:
                f.write(f"{field}\n")
        
        client.close()
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise Exception("Failed to connect to MongoDB")
    
    cmd = [
        "mongoexport",
        f"--uri={MONGO_URI}",
        f"--db={MONGO_DB}",
        f"--collection={MONGO_COLLECTION}",
        "--type=csv",
        f"--fieldFile={fields_file}",
        f"--out={LOCAL_CSV}",
    ]
    
    print("Running mongoexport...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"mongoexport stderr: {result.stderr}")
        raise Exception("mongoexport failed")
    else:
        print(f"mongoexport succeeded. File created at {LOCAL_CSV}")

if __name__ == "__main__":
    extract_function()