from datetime import datetime, timedelta
import os
import hashlib
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from pymongo import MongoClient
from utils.config import config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'mongodb_backup_dag',
    default_args=default_args,
    description='MongoDB backup pipeline',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    tags=['backup', 'mongodb', 'minio'],
)

MONGO_URI = config["mongo_uri"]
MONGO_DB = config["mongo_db"]
#.yaml
MONGO_COLLECTION = "divar-dataset_1"

BACKUP_DIR = './git/delta-data-pipelines/dags/backup/mongodb_backup'
os.makedirs(BACKUP_DIR, exist_ok=True)

mongodump_bin = "mongodump"

# MinIO configuration
MINIO_ENDPOINT = config["minio_endpoint"]
MINIO_ACCESS_KEY = config["minio_access_key"]
MINIO_SECRET_KEY = config["minio_secret_key"]
MINIO_BUCKET = config["minio_bucket"]

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure bucket exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    
# Helpers
def compute_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def list_backups_in_minio():
    objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)
    keys = [obj.object_name for obj in objects]
    # extract dates from keys
    dates = sorted(set([k.split('/')[0] for k in keys]))
    return dates

# Tasks
def full_backup(**kwargs):
    today_str = datetime.now().strftime("%Y%m%d")
    backup_path = os.path.join(BACKUP_DIR, f'full_backup_{today_str}')
    os.makedirs(backup_path, exist_ok=True)

    # Run mongodump
    cmd = f'{mongodump_bin} --uri="{MONGO_URI}" --db={MONGO_DB} --collection={MONGO_COLLECTION} --out={backup_path}'
    subprocess.run(cmd, shell=True, check=True)
    print(f"Full backup done: {backup_path}")

    # Upload to MinIO
    for root, dirs, files in os.walk(backup_path):
        for file in files:
            full_file_path = os.path.join(root, file)
            key = f"{today_str}/{file}"
            minio_client.fput_object(MINIO_BUCKET, key, full_file_path)
    print(f"Backup uploaded to MinIO: {today_str}")

    # keep last 3 backups
    dates = list_backups_in_minio()
    while len(dates) > 3:
        oldest = dates.pop(0)
        # delete all files for oldest date
        objects_to_delete = minio_client.list_objects(MINIO_BUCKET, prefix=f"{oldest}/", recursive=True)
        for obj in objects_to_delete:
            minio_client.remove_object(MINIO_BUCKET, obj.object_name)
        print(f"Deleted old backup from MinIO: {oldest}")
        
# def validate_backup(**kwargs):
#     today_str = datetime.now().strftime("%Y%m%d")
#     path = os.path.join(BACKUP_DIR, f'full_backup_{today_str}', MONGO_DB, f'{MONGO_COLLECTION}.bson')
#     if not os.path.exists(path):
#         raise ValueError(f"Backup file not found: {path}")

#     checksum = compute_checksum(path)
#     prev = Variable.get('backup_checksum', default_var=None)
#     if prev and prev == checksum:
#         print("Warning: checksum same as previous!")
#     Variable.set('backup_checksum', checksum)

#     # Validation
#     temp_db = 'temp_val_db'
#     base_dir = os.path.dirname(path)
#     cmd = f'mongorestore --uri="{MONGO_URI}" --db={temp_db} --drop {base_dir}'
#     subprocess.run(cmd, shell=True, check=True)

#     client = MongoClient(MONGO_URI)
#     count = client[temp_db].get_collection(MONGO_COLLECTION).count_documents({})
#     if count == 0:
#         raise ValueError("Validation failed: no documents restored")
#     client.drop_database(temp_db)
#     print("Validation passed.")
def validate_backup(**kwargs):
    today_str = datetime.now().strftime("%Y%m%d")
    path = os.path.join(BACKUP_DIR, f'full_backup_{today_str}', MONGO_DB, f'{MONGO_COLLECTION}.bson')
    
    if not os.path.exists(path):
        raise ValueError(f"Backup file not found: {path}")
    
    checksum = compute_checksum(path)
    prev = Variable.get('backup_checksum', default_var=None)
    if prev and prev == checksum:
        print("Warning: checksum same as previous!")
    Variable.set('backup_checksum', checksum)
    
    # Validation 
    restore_collection = "divar-dataset_1_restore"
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    if restore_collection in db.list_collection_names():
        db[restore_collection].delete_many({}) 
    
    # mongorestore
    cmd = (
        f'mongorestore --uri="{MONGO_URI}" '
        f'--db={MONGO_DB} '
        f'--collection={restore_collection} '
        f'--noIndexRestore '
        f'{path}'
    )
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("mongorestore error output:")
        print(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    
    count = db[restore_collection].count_documents({})
    print(f"Restored documents count in {restore_collection}: {count}")
    
    if count == 0:
        raise ValueError("Validation failed: no documents restored")
    
    print(f"Validation passed. Data is available in collection: {restore_collection}")
    print("کالکشن divar-dataset_1_restore پاک نمی‌شود و می‌توانید آن را مشاهده کنید.")
    
def cleanup_task_fn(**kwargs):
    subprocess.run(f'rm -rf {BACKUP_DIR}/*', shell=True)
    print("Local backup directory cleaned.")
    
full_task = PythonOperator(task_id='full_backup', python_callable=full_backup, dag=dag)
validate_task = PythonOperator(task_id='validate_backup', python_callable=validate_backup, dag=dag)
cleanup_task = PythonOperator(task_id='cleanup_old_backups', python_callable=cleanup_task_fn, dag=dag)

# DAG Flow
full_task >> validate_task >> cleanup_task
# full_task >> cleanup_task