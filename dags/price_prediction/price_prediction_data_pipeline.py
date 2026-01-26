from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import os
import pandas as pd
import requests
# from pymongo import MongoClient
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

def extract_from_mongo(**context):
    os.makedirs(TMP_DIR, exist_ok=True)

    print(f"[extract] TMP_DIR ensured: {TMP_DIR}")
    
    cmd = [
        "mongoexport",
        f"--uri={MONGO_URI}",
        f"--db={MONGO_DB}",
        f"--collection={MONGO_COLLECTION}",
        # "--type=json",
        # "--jsonArray=false",
        "--type=csv",
        "--fields=cat_1,cat_2",
        f"--out={LOCAL_CSV}",
        # f"--out={LOCAL_CSV}.json",
    ]
    
    # client = MongoClient(MONGO_URI)
    # db = client[MONGO_DB]
    # collection = db[MONGO_COLLECTION]

    # data = list(collection.find({}, {"_id": 0}))
    # if not data:
    #     raise AirflowFailException("Mongo collection is empty")

    # df = pd.DataFrame(data)
    # df.to_csv(LOCAL_CSV, index=False)

    # print(f"Extracted {len(df)} rows from MongoDB")
    print(f"[extract] Running mongoexport...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[extract] mongoexport stderr: {result.stderr}")
        raise AirflowFailException("mongoexport failed")
    else:
        print(f"[extract] mongoexport succeeded. File created at {LOCAL_CSV}")

# def transform_data(**context):
#     df = pd.read_csv(LOCAL_CSV)
#     print(f"[transform] CSV loaded, {len(df)} rows")

#     df = df[df['cat3_slug'] == 'apartment-sell'].copy()
#     print(f"[transform] Filtered cat3_slug=='apartment-sell', {len(df)} rows remain")
    
#     drop_cols = (
#         ['_id', 'created_at', 'post_token', 'content_url'] +
#         [f"images[{i}]" for i in range(21)]
#     )

#     df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)
#     print(f"[transform] Dropped unnecessary columns, {len(df.columns)} columns remain")

#     if 'construction_year' in df.columns:
#         df['construction_year'] = df['construction_year'].replace(-1370, 1369)

#     df.to_csv(LOCAL_CSV, index=False)
#     print(f"[transform] Transformation completed. File saved at {LOCAL_CSV}")
    
#     if df.empty:
#         raise AirflowFailException(
#             "No data left after filtering cat3_slug == apartment-sell"
#         )
def transform_data(**context):
    chunksize = 50000  
    processed_chunks = 0
    total_rows = 0

    tmp_transformed_csv = f"{LOCAL_CSV}.tmp"

    # اگر فایل tmp وجود داشت، پاکش کن
    if os.path.exists(tmp_transformed_csv):
        os.remove(tmp_transformed_csv)

    print(f"[transform] Starting transformation in chunks...")

    for chunk in pd.read_json(f"{LOCAL_CSV}.json", lines=True, chunksize=chunksize):
        print(f"[transform] Processing chunk with {len(chunk)} rows")
        if 'cat3_slug' not in chunk.columns:
            print("[transform] cat3_slug not found in this chunk, skipping")
            continue
    
        chunk = chunk[chunk['cat3_slug'] == 'apartment-sell'].copy()

        drop_cols = (
            # ['_id', 'created_at', 'post_token', 'content_url']
            ['_id', 'created_at', 'post_token', 'content_url'] +
            [f"images[{i}]" for i in range(21)]
        )
        chunk.drop(columns=[c for c in drop_cols if c in chunk.columns], inplace=True)

        if 'construction_year' in chunk.columns:
            chunk['construction_year'] = chunk['construction_year'].replace(-1370, 1369)

        # append to tmp CSV
        if os.path.exists(tmp_transformed_csv):
            chunk.to_csv(tmp_transformed_csv, index=False, mode='a', header=False)
        else:
            chunk.to_csv(tmp_transformed_csv, index=False, mode='w', header=True)

        processed_chunks += 1
        total_rows += len(chunk)
        print(f"[transform] Chunk processed, {len(chunk)} rows kept")

    # جایگزین کردن CSV اصلی با CSV پردازش شده
    os.replace(tmp_transformed_csv, LOCAL_CSV)
    print(f"[transform] Transformation completed. Total rows after filter: {total_rows}")

    if total_rows == 0:
        raise AirflowFailException(
            "No data left after filtering cat3_slug == apartment-sell"
        )

def upload_to_minio(**context):
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

def validate_upload(**context):
    minio_client = get_minio_client()
    bucket = MINIO_PRICE_PREDICTION_BUCKET

    try:
        minio_client.stat_object(bucket, LAST_DATA_PATH)
        print("[validate] Upload validation successful")
    except S3Error:
        raise AirflowFailException("Upload validation failed")


def call_model_endpoint(**context):
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

    response.raise_for_status()

    try:
        metrics = response.json()
        print("Model training metrics:")
        print(metrics)
    except ValueError:
        print("Response is not JSON")

with DAG(
    dag_id="price_prediction",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mongo", "minio", "ml"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_from_mongo",
        python_callable=extract_from_mongo,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    validate_task = PythonOperator(
        task_id="validate_upload",
        python_callable=validate_upload,
    )

    call_model = PythonOperator(
        task_id="call_model_endpoint",
        python_callable=call_model_endpoint,
    )

    extract_task >> transform_task >> load_task >> validate_task >> call_model
