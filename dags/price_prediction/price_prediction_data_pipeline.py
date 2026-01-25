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

CSV_NAME = "residential_sell.csv"

TMP_DIR = "/tmp/price_prediction"
LOCAL_CSV = f"{TMP_DIR}/dataset.csv"

MINIO_PREFIX = "v2"
LAST_DATA_PATH = f"{MINIO_PREFIX}/last-data/{CSV_NAME}"
OLD_DATA_PREFIX = f"{MINIO_PREFIX}/old-data"

def extract_from_mongo(**context):
    os.makedirs(TMP_DIR, exist_ok=True)

    client = MongoClient(os.environ["MONGO_URI"])
    collection = client[
        os.environ["MONGO_DB"]
    ][os.environ["MONGO_COLLECTION"]]

    data = list(collection.find({}, {"_id": 0}))
    if not data:
        raise AirflowFailException("Mongo collection is empty")

    df = pd.DataFrame(data)
    df.to_csv(LOCAL_CSV, index=False)

    print(f"Extracted {len(df)} rows from MongoDB")


def transform_data(**context):
    df = pd.read_csv(LOCAL_CSV)

    df = df[df['cat3_slug'] == 'apartment-sell'].copy()

    drop_cols = (
        ['_id', 'created_at', 'post_token', 'content_url'] +
        [f"images[{i}]" for i in range(21)]
    )

    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    if 'construction_year' in df.columns:
        df['construction_year'] = df['construction_year'].replace(-1370, 1369)

    df.to_csv(LOCAL_CSV, index=False)
    print(f"Transformation completed. Rows after filter: {len(df)}")


def upload_to_minio(**context):
    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )

    bucket = os.environ["MINIO_PRICE_PREDICTION_BUCKET"]

    # 1- if last-data exist → old-data
    try:
        minio_client.stat_object(bucket, LAST_DATA_PATH)

        old_key = f"{OLD_DATA_PREFIX}/{CSV_NAME}"

        minio_client.copy_object(
            bucket,
            old_key,
            f"/{bucket}/{LAST_DATA_PATH}",
        )

        minio_client.remove_object(bucket, LAST_DATA_PATH)
        print("Previous last-data moved to old-data")

    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        print("No previous last-data found")

    # 2- upload new data
    minio_client.fput_object(
        bucket,
        LAST_DATA_PATH,
        LOCAL_CSV,
        content_type="text/csv"
    )

    print("New data uploaded to last-data")


def validate_upload(**context):
    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )

    bucket = os.environ["MINIO_PRICE_PREDICTION_BUCKET"]

    try:
        minio_client.stat_object(bucket, LAST_DATA_PATH)
        print("Upload validation successful")
    except S3Error:
        raise AirflowFailException("Upload validation failed")


def call_model_endpoint(**context):
    url = os.environ["MODEL_ENDPOINT_URL"]

    response = requests.post(url, timeout=300)
    response.raise_for_status()

    metrics = response.json()
    print("Model training metrics:")
    print(metrics)


with DAG(
    dag_id="price_prediction",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mongo", "minio", "ml"],
) as dag:

    extract = PythonOperator(
        task_id="extract_from_mongo",
        python_callable=extract_from_mongo,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    validate = PythonOperator(
        task_id="validate_upload",
        python_callable=validate_upload,
    )

    call_model = PythonOperator(
        task_id="call_model_endpoint",
        python_callable=call_model_endpoint,
    )

    extract >> transform >> upload >> validate >> call_model
