from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import os
import pandas as pd
import requests
from pymongo import MongoClient
from minio import Minio
from minio.error import S3Error
from utils.config import config
from minio.commonconfig import CopySource
import subprocess

MINIO_ENDPOINT = config["minio_endpoint"]
MINIO_ACCESS_KEY = config["minio_access_key"]
MINIO_SECRET_KEY = config["minio_secret_key"]
MINIO_PRICE_PREDICTION_BUCKET = config["minio_price_prediction_bucket"]

MONGO_URI = config["mongo_uri"]
MONGO_DB = config["mongo_db"]
# MONGO_COLLECTION_ENV = config["mongo_collection"]
# MONGO_COLLECTION = f'divar-{MONGO_COLLECTION_ENV}'
MONGO_COLLECTION = "divar-dataset"

ANALYTICS_ENDPOINT_URL = f"{config['analytics_endpoint_url']}/api/v1/update-model"
ANALYTICS_ENDPOINT_ACCESS_TOKEN = config["analytics_endpoint_access_token"]

CSV_NAME = "residential_sell.csv"

TMP_DIR = "/tmp/price_prediction"
LOCAL_CSV = f"{TMP_DIR}/{CSV_NAME}"

MINIO_PREFIX = "v2"
LAST_DATA_PATH = f"{MINIO_PREFIX}/last-data/{CSV_NAME}"
OLD_DATA_PREFIX = f"{MINIO_PREFIX}/old-data"

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

def extract_function(**context):
    os.makedirs(TMP_DIR, exist_ok=True)

    try:
        print(f"TMP_DIR ensured: {TMP_DIR}")
        
        # get fields
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        sample_doc = collection.find_one()
        if not sample_doc:
            raise AirflowFailException("Mongo collection is empty")
        
        EXCLUDED_FIELDS = ['_id', 'created_at', 'post_token', 'content_url', 'images']
        # EXCLUDED_FIELDS = ['_id', 'created_at', 'post_token', 'content_url']

        fields = [key for key in sample_doc.keys() if key not in EXCLUDED_FIELDS]
        
        print(f"Found {len(fields)} fields after excluding {EXCLUDED_FIELDS}")
        # print(f"[extract] First 10 fields: {fields[:10]}")
        # print(f"[extract] Last 10 fields: {fields[-10:]}")
            
        fields_file = f"{TMP_DIR}/fields.txt"
        with open(fields_file, 'w') as f:
            for field in fields:
                f.write(f"{field}\n")
        
        client.close()
        
    except Exception as e:
        print(f"[extract] Error connecting to MongoDB: {e}")
        raise AirflowFailException("Failed to connect to MongoDB")
    
    cmd = [
        "mongoexport",
        f"--uri={MONGO_URI}",
        f"--db={MONGO_DB}",
        f"--collection={MONGO_COLLECTION}",
        "--type=csv",
        f"--fieldFile={fields_file}",
        f"--out={LOCAL_CSV}",
    ]
    
    print(f"[extract] Running mongoexport...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[extract] mongoexport stderr: {result.stderr}")
        raise AirflowFailException("mongoexport failed")
    else:
        print(f"[extract] mongoexport succeeded. File created at {LOCAL_CSV}")

def transform_function(**context):
    chunksize = 50000  
    processed_chunks = 0
    total_rows = 0

    tmp_transformed_csv = f"{LOCAL_CSV}.tmp"

    if os.path.exists(tmp_transformed_csv):
        os.remove(tmp_transformed_csv)

    print(f"[transform] Starting transformation in chunks...")

    for chunk in pd.read_csv(LOCAL_CSV, chunksize=chunksize):
        print(f"[transform] Processing chunk with {len(chunk)} rows")
        if 'cat3_slug' not in chunk.columns:
            print("[transform] cat3_slug not found in this chunk, skipping")
            continue
    
        chunk = chunk[chunk['cat3_slug'] == 'apartment-sell'].copy()

        if 'construction_year' in chunk.columns:
            chunk['construction_year'] = chunk['construction_year'].replace(-1370, 1369)

        if os.path.exists(tmp_transformed_csv):
            chunk.to_csv(tmp_transformed_csv, index=False, mode='a', header=False)
        else:
            chunk.to_csv(tmp_transformed_csv, index=False, mode='w', header=True)

        processed_chunks += 1
        total_rows += len(chunk)
        print(f"[transform] Chunk processed, {len(chunk)} rows kept")

    os.replace(tmp_transformed_csv, LOCAL_CSV)
    print(f"[transform] Transformation completed. Total rows after filter: {total_rows}")

    if total_rows == 0:
        raise AirflowFailException(
            "No data left after filtering cat3_slug == apartment-sell"
        )

def load_function(**context):
    minio_client = get_minio_client()
    bucket = MINIO_PRICE_PREDICTION_BUCKET

    # 1- if last-data exist → old-data
    try:
        minio_client.stat_object(bucket, LAST_DATA_PATH)

        old_key = f"{OLD_DATA_PREFIX}/{CSV_NAME}"

        source = CopySource(
            bucket_name=bucket,
            object_name=LAST_DATA_PATH,
        )

        minio_client.copy_object(
            bucket,
            old_key,
            source,
        )

        minio_client.remove_object(bucket, LAST_DATA_PATH)
        print("[upload] Previous last-data moved to old-data")

    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        print("[upload] No previous last-data found")

    # 2- upload new data
    minio_client.fput_object(
        bucket,
        LAST_DATA_PATH,
        LOCAL_CSV,
        content_type="text/csv"
    )

    print("[upload] New data uploaded to last-data")

def validate_function(**context):
    minio_client = get_minio_client()
    bucket = MINIO_PRICE_PREDICTION_BUCKET

    try:
        obj_info = minio_client.stat_object(bucket, LAST_DATA_PATH)
        
        if obj_info.size == 0:
            raise AirflowFailException("Uploaded file is empty (0 bytes)")
            
        print(f"[validate] Upload validation successful - File size: {obj_info.size} bytes")
        
    except S3Error as e:
        if e.code == "NoSuchKey":
            raise AirflowFailException(f"File {LAST_DATA_PATH} not found in MinIO")
        else:
            raise AirflowFailException(f"MinIO error: {e.message}")
        
def call_model_function(**context):
    url = ANALYTICS_ENDPOINT_URL

    headers = {
        "Authorization": f"Bearer {ANALYTICS_ENDPOINT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        url,
        headers=headers,
        timeout=(10, 300)
    )

    print(f"[call_model] Endpoint response status: {response.status_code}")
    print(f"[call_model] Endpoint response body: {response.text}")

    # check status code
    if response.status_code != 200:
        raise AirflowFailException(f"Endpoint returned status: {response.status_code}")

    try:
        result = response.json()
        print("Model training result:")
        print(result)
        
        # check success flag
        if isinstance(result, dict) and result.get('success') is False:
            error_msg = result.get('message', 'Training failed')
            
            
            try:
                minio_client = get_minio_client()
                bucket = MINIO_PRICE_PREDICTION_BUCKET
                
                # 1. del last-data 
                try:
                    minio_client.remove_object(bucket, LAST_DATA_PATH)
                    print("[call_model] Removed new last-data (training failed)")
                except S3Error as e:
                    if e.code != "NoSuchKey":
                        print(f"[call_model] Warning: Could not remove last-data: {e}")
                
                # 2. old-data -> last-data
                old_key = f"{OLD_DATA_PREFIX}/{CSV_NAME}"
                try:
                    
                    minio_client.stat_object(bucket, old_key)
                    
                    source = CopySource(
                        bucket_name=bucket,
                        object_name=old_key,
                    )
                    
                    minio_client.copy_object(
                        bucket,
                        LAST_DATA_PATH,
                        source,
                    )
                    
                    print("[call_model] Restored old-data to last-data")
                    
                except S3Error as e:
                    if e.code == "NoSuchKey":
                        print("[call_model] Warning: No old-data found to restore")
                    else:
                        print(f"[call_model] Warning: Could not restore old-data: {e}")
                
            except Exception as e:
                print(f"[call_model] Error during rollback: {e}")
            
            raise AirflowFailException(f"Model training failed: {error_msg}")
            
        # if success: true 
        if isinstance(result, dict) and result.get('success') is True:
            print("✅ Model training completed successfully")
            
    except ValueError:
        raise AirflowFailException("Response is not valid JSON")

def predict_test_function(**context):

    predict_url = f"{config['analytics_endpoint_url']}/api/v1/predict"
    
    headers = {
        "Authorization": f"Bearer {ANALYTICS_ENDPOINT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    
    # static data
    static_data = {
        "neighborhood_name": "velenjak",
        "location_latitude": 0,
        "location_longitude": 0,
        "building_size": 100,
        "rooms_count": 0,
        "floor": 0,
        "total_floors_count": 0,
        "has_balcony": 0,
        "has_elevator": 0,
        "has_parking": 0,
        "is_rebuilt": 0,
        "building_age": 0,
        "has_warm_water_provider_powerhouse": 0,
        "has_heating_system_shoofaj": 0,
        "has_cooling_system_duct_split": 0,
        "has_cooling_system_water_cooler": 0,
        "building_direction_north": 0,
        "building_direction_south": 0,
        "floor_material_ceramic": 0,
        "floor_material_stone": 0,
        "months_since_2020": 0
    }
    
    print(f"[predict_test] Calling predict endpoint: {predict_url}")
    print(f"[predict_test] Request data: {static_data}")
    
    try:
        response = requests.post(
            predict_url,
            headers=headers,
            json=static_data,
            timeout=(10, 60)
        )
        
        print(f"[predict_test] Response status: {response.status_code}")
        print(f"[predict_test] Response body: {response.text}")
        
        #  1: status code
        if response.status_code != 200:
            raise AirflowFailException(
                f"Predict endpoint returned status: {response.status_code}"
            )
        
        #  2: valid JSON 
        try:
            result = response.json()
        except ValueError:
            raise AirflowFailException("Predict response is not valid JSON")
        
        #  3: key check
        if not isinstance(result, dict):
            raise AirflowFailException("Predict response is not a JSON object")
        
        if "total_price" not in result:
            raise AirflowFailException("Predict response missing 'total_price' field")
        
        # 4: total_price 
        total_price = result.get("total_price")
        if total_price is None:
            raise AirflowFailException("Total price is null in response")
        
        if not isinstance(total_price, (int, float)):
            raise AirflowFailException(f"Total price is not a number: {total_price}")
        
        if total_price <= 10000000000: 
            raise AirflowFailException(
                f"Total price ({total_price:,}) is less than or equal to 10 billion"
            )
        
        # check if price_per_sqm_prediction exist
        # if "price_per_sqm_prediction" in result:
        #     price_per_sqm = result.get("price_per_sqm_prediction")
        #     print(f"[predict_test] Price per square meter: {price_per_sqm:,}")
        
        # print(f"[predict_test] Total price: {total_price:,}")
        print("✅ Predict test passed successfully")
        
    except requests.exceptions.RequestException as e:
        raise AirflowFailException(f"Failed to call predict endpoint: {str(e)}")

with DAG(
    dag_id="price_prediction",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mongo", "minio", "ml"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_function,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_function,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_function,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate_function,
    )

    call_model_task = PythonOperator(
        task_id="call_model_task",
        python_callable=call_model_function,
    )

    predict_test_task = PythonOperator(
        task_id="predict_test_task",
        python_callable=predict_test_function,
    )
    
    extract_task >> transform_task >> load_task >> validate_task >> call_model_task >> predict_test_task
