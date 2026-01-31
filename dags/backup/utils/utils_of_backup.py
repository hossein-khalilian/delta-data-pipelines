from datetime import datetime
import os
import subprocess
from airflow.exceptions import AirflowFailException
from minio import Minio
from pymongo import MongoClient
from utils.config import config

# CONFIG 
MONGO_URI = config["mongo_uri"]
MONGO_URI_RESTORE = config["mongo_uri_restore"]
MONGO_DB = config["mongo_db"]
MONGO_DB_RESTORE = config["mongo_db_restore"]

BACKUP_DIR = "./delta-data-pipelines/dags/backup/mongodb_backup"

MINIO_ENDPOINT = config["minio_endpoint"]
MINIO_ACCESS_KEY = config["minio_access_key"]
MINIO_SECRET_KEY = config["minio_secret_key"]
MINIO_BACKUP_BUCKET = config["minio_backup_bucket"]

mongodump_bin = "mongodump"
mongorestore_bin = "mongorestore"

os.makedirs(BACKUP_DIR, exist_ok=True)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

if not minio_client.bucket_exists(MINIO_BACKUP_BUCKET):
    minio_client.make_bucket(MINIO_BACKUP_BUCKET)

# HELPERS
def list_backup_dates():
    objs = minio_client.list_objects(MINIO_BACKUP_BUCKET, recursive=True)
    return sorted(set(o.object_name.split("/")[0] for o in objs))

# TASK FUNCTIONS
def full_backup_function(**context):
    print(f"[BACKUP] Starting full backup for DB={MONGO_DB}")
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
    
    print(f"[BACKUP] mongodump finished. Local path: {backup_path}")

    for root, _, files in os.walk(backup_path):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, backup_path)
            minio_client.fput_object(
                MINIO_BACKUP_BUCKET,
                f"{backup_date}/{rel_path}",
                local_path,
            )
            
    print(f"[BACKUP] Backup uploaded to MinIO. Date={backup_date}")

    context["ti"].xcom_push(key="backup_date", value=backup_date)

def restore_function(**context):
    backup_date = context["ti"].xcom_pull(key="backup_date")
    local_backup_path = os.path.join(BACKUP_DIR, backup_date)
    
    if not os.path.exists(local_backup_path):
        raise AirflowFailException("Local backup path does not exist")
    
    dump_db_path = os.path.join(local_backup_path, MONGO_DB)
    
    print(f"[RESTORE] Restoring from: {dump_db_path}")
    print(f"[RESTORE] Target DB: {MONGO_DB_RESTORE}")
    
    cmd = (
        f'{mongorestore_bin} '
        f'--uri="{MONGO_URI_RESTORE}" '
        f'--gzip '
        f'--drop '
        f'--nsFrom="{MONGO_DB}.*" '
        f'--nsTo="{MONGO_DB_RESTORE}.*" '
        f'"{dump_db_path}"'
    )

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise AirflowFailException(f"mongorestore failed:\n{result.stderr}")

    print(f"[RESTORE] mongorestore completed successfully")


def validate_function(**context):

    # Mongo clients 
    client_source = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    source_db = client_source[MONGO_DB]

    client_restore = MongoClient(MONGO_URI_RESTORE, serverSelectionTimeoutMS=5000)
    restore_db = client_restore[MONGO_DB_RESTORE]

    src_cols = set(source_db.list_collection_names())
    restored_cols = set(restore_db.list_collection_names())

    print(f"[VALIDATE] Source collections: {len(src_cols)}")
    print(f"[VALIDATE] Restored collections: {len(restored_cols)}")

    if not restored_cols:
        raise AirflowFailException("Restore DB is empty after restore")

    ignore = {"system.profile"}
    if src_cols - ignore != restored_cols - ignore:
        raise AirflowFailException(
            f"Collection mismatch. "
            f"source={len(src_cols)} restored={len(restored_cols)}"
        )

    for col in restored_cols:
        print(f"[VALIDATE] Checking collection: {col}")
        if restore_db[col].find_one() is None:
            raise AirflowFailException(f"{col} is empty after restore")

    client_source.close()
    client_restore.close()
    
    print(
        f"Validation successful: "
        f"{len(restored_cols)} collections restored correctly."
    )

def cleanup_function(**context):
    backup_date = context["ti"].xcom_pull(key="backup_date")
    
    print(f"[CLEANUP] Removing local backup for date={backup_date}")

    # cleanup local
    if BACKUP_DIR and BACKUP_DIR != "/":
        subprocess.run(
            ["rm", "-rf", os.path.join(BACKUP_DIR, backup_date)],
            check=True
        )

    # keep only last 3 backups in MinIO
    dates = list_backup_dates()
    print(f"[CLEANUP] Keeping last 3 backups. Current backups: {dates}")
    while len(dates) > 3:
        oldest = dates.pop(0)
        print(f"[CLEANUP] Removing old backup from MinIO: {oldest}")
        for obj in minio_client.list_objects(
            MINIO_BACKUP_BUCKET, prefix=f"{oldest}/", recursive=True
        ):
            minio_client.remove_object(MINIO_BACKUP_BUCKET, obj.object_name)
