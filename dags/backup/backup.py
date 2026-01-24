from datetime import datetime, timedelta
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from minio import Minio
from pymongo import MongoClient
from utils.config import config
import gzip

# -------------------- CONFIG --------------------
MONGO_URI = config["mongo_uri"]
MONGO_DB = config["mongo_db"]
# TMP_RESTORE_DB = f"{MONGO_DB}-restore"
RESTORE_DB = "datasets-restore" 

BACKUP_DIR = "./delta-data-pipelines/dags/backup/mongodb_backup"

MINIO_ENDPOINT = config["minio_endpoint"]
MINIO_ACCESS_KEY = config["minio_access_key"]
MINIO_SECRET_KEY = config["minio_secret_key"]
MINIO_BUCKET = config["minio_bucket"]

mongodump_bin = "mongodump"
mongorestore_bin = "mongorestore"

os.makedirs(BACKUP_DIR, exist_ok=True)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

# -------------------- DAG --------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dag = DAG(
    "mongodb_full_backup",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    tags=["mongodb", "backup", "minio"],
)

# -------------------- HELPERS --------------------
def list_backup_dates():
    objs = minio_client.list_objects(MINIO_BUCKET, recursive=True)
    return sorted(set(o.object_name.split("/")[0] for o in objs))

# -------------------- TASKS --------------------
def full_backup(**context):
    backup_date = datetime.now().strftime("%Y%m%d")
    backup_path = os.path.join(BACKUP_DIR, backup_date)

    os.makedirs(backup_path, exist_ok=True)

    cmd = (
        f'{mongodump_bin} '
        f'--uri="{MONGO_URI}" '
        f'--db="{MONGO_DB}" '
        f'--gzip '
        f'--out="{backup_path}"'
    )
    subprocess.run(cmd, shell=True, check=True)

    for root, _, files in os.walk(backup_path):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, backup_path)
            minio_client.fput_object(
                MINIO_BUCKET,
                f"{backup_date}/{rel_path}",
                local_path,
            )

    context["ti"].xcom_push(key="backup_date", value=backup_date)

def validate_backup(**context):
    backup_date = context["ti"].xcom_pull(key="backup_date")
    local_backup_path = os.path.join(BACKUP_DIR, backup_date)

    client = MongoClient(MONGO_URI)
    source_db = client[MONGO_DB]
    restore_db = client[RESTORE_DB]

    print(f"Clearing existing collections in {RESTORE_DB} before restore...")
    for col in restore_db.list_collection_names():
        restore_db.drop_collection(col)

    # if TMP_RESTORE_DB in client.list_database_names():
    #     client.drop_database(TMP_RESTORE_DB) # خط حذف دیتابیس حذف شد

    # به‌روزرسانی دستور mongorestore برای استفاده از RESTORE_DB
    cmd = (
        f'{mongorestore_bin} '
        f'--uri="{MONGO_URI}" '
        f'--gzip '
        f'--nsFrom="{MONGO_DB}.*" '
        f'--nsTo="{RESTORE_DB}.*" '
        f'"{local_backup_path}"'
    )

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise AirflowFailException(result.stderr)

    restored_db = client[RESTORE_DB]

    src_cols = set(source_db.list_collection_names())
    restored_cols = set(restored_db.list_collection_names())

    if src_cols != restored_cols:
        raise AirflowFailException("Collection count mismatch")

    for col in restored_cols:
        if restored_db[col].count_documents({}) < 1:
            raise AirflowFailException(f"Collection {col} is empty")

# def validate_backup(**context):
#     backup_date = context["ti"].xcom_pull(key="backup_date")
#     backup_path = os.path.join(BACKUP_DIR, backup_date)

#     if not os.path.exists(backup_path):
#         raise AirflowFailException("Backup path does not exist")

#     files = []
#     for root, _, fs in os.walk(backup_path):
#         for f in fs:
#             files.append(os.path.join(root, f))

#     if not files:
#         raise AirflowFailException("Backup directory is empty")

#     bson_files = [f for f in files if f.endswith(".bson.gz")]
#     meta_files = [f for f in files if f.endswith(".metadata.json.gz")]

#     if not bson_files:
#         raise AirflowFailException("No .bson.gz files found")

#     if not meta_files:
#         raise AirflowFailException("No metadata (.metadata.json.gz) files found")

#     for f in bson_files[:3]: 
#         try:
#             with gzip.open(f, "rb") as g:
#                 g.read(1024)
#         except Exception:
#             raise AirflowFailException(f"Corrupted gzip file: {f}")

#     print(
#         f"Validation passed: "
#         f"{len(bson_files)} bson files, "
#         f"{len(meta_files)} metadata files"
#     )

def cleanup_task(**context):
    backup_date = context["ti"].xcom_pull(key="backup_date")

    # cleanup local
    subprocess.run(f"rm -rf {BACKUP_DIR}/*", shell=True)

    # keep only last 3 backups in MinIO
    dates = list_backup_dates()
    while len(dates) > 3:
        oldest = dates.pop(0)
        for obj in minio_client.list_objects(
            MINIO_BUCKET, prefix=f"{oldest}/", recursive=True
        ):
            minio_client.remove_object(MINIO_BUCKET, obj.object_name)

# -------------------- OPERATORS --------------------
backup_task = PythonOperator(
    task_id="full_backup",
    python_callable=full_backup,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_backup",
    python_callable=validate_backup,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup_task,
    dag=dag,
)

backup_task >> validate_task >> cleanup_task
