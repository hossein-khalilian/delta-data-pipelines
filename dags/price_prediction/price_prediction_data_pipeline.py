from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from minio import Minio
import pandas as pd

BASE_PATH = 
EXPORT_DIR = 
EXPORT_FILE = 
PROCESSED_FILE =


def preprocess_divar_data():
    print(f"start {EXPORT_FILE}")

    if not EXPORT_FILE.exists():
        raise FileNotFoundError(f"not found: {EXPORT_FILE}")

    df = pd.read_csv(EXPORT_FILE, low_memory=False)

    df = df[df["cat3_slug"] == "apartment-sell"].copy()

    columns_to_drop = [
        "_id", "created_at", "post_token", "content_url"
    ]
    image_cols = [f"images[{i}]" for i in range(21)]  
    columns_to_drop.extend([col for col in image_cols if col in df.columns])

    df = df.drop(columns=columns_to_drop, errors="ignore")

    if "construction_year" in df.columns:
        df["construction_year"] = df["construction_year"].replace(-1370, 1369)

        # df = df[df["construction_year"].between(1300, 1405)] 
    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_FILE, index=False, encoding="utf-8-sig")

    print(f"Processed file successfully saved at: {PROCESSED_FILE}")
    print(f"Final record count: {len(df):,}")


def upload_to_minio():
    if not PROCESSED_FILE.exists():
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}")
    
    minio_conn = BaseHook.get_connection("minio_divar_conn")

    client = Minio(
        endpoint=minio_conn.extra_dejson.get("endpoint", "localhost:9000"),
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=minio_conn.extra_dejson.get("secure", False),
    )

    bucket_name = "divar_dataset_train"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket created: {bucket_name}")

    object_name = f"train/apartment_sell/processed_divar_{datetime.now().strftime('%Y%m%d')}.csv"

    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=str(PROCESSED_FILE),
        content_type="text/csv",
    )

    print(f"File successfully uploaded: {object_name}")

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="divar_apartment_sell_export_preprocess_minio",
    default_args=default_args,
    description="Export → Preprocess → Upload Divar apartment-sell dataset to MinIO",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["divar", "etl", "price-prediction", "minio"],
    max_active_runs=1,
) as dag:

    export_mongo = BashOperator(
        task_id="export_from_mongodb",
        bash_command=f"""
            mkdir -p {EXPORT_DIR} &&
            mongoexport --uri="{BaseHook.get_connection('mongo_divar').get_uri()}" \
                        --collection=divar-datasets_1 \
                        --type=csv \
                        --fields='cat3_slug,construction_year,price,...' \  
                        --out={EXPORT_FILE} \
                        --quiet
        """,
        cwd="/tmp",  
    )

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_divar_data,
    )

    upload_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    export_mongo >> preprocess >> upload_minio