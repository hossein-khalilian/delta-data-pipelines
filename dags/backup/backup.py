from datetime import datetime, timedelta
import os
import hashlib
import subprocess
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator

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
    tags=['backup', 'mongodb'],
)

MONGO_URI = config["mongo_uri"]
MONGO_DB = config["mongo_db"]
#.yaml
MONGO_COLLECTION = "divar-dataset_1"

BACKUP_DIR = './git/delta-data-pipelines/dags/backup/mongodb_backup'
os.makedirs(BACKUP_DIR, exist_ok=True)

mongodump_bin = "mongodump"

def full_backup(**kwargs):
    cmd = f'{mongodump_bin} --uri="{MONGO_URI}" --db={MONGO_DB} --collection={MONGO_COLLECTION} --out={BACKUP_DIR}/full_backup'
    subprocess.run(cmd, shell=True, check=True)
    
    print("Full backup done.")

def compute_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def validate_backup(**kwargs):
    # ti = kwargs['ti']
    # branch_result = ti.xcom_pull(task_ids='branch_task')
    
    path = os.path.join(BACKUP_DIR, 'full_backup', MONGO_DB, f'{MONGO_COLLECTION}.bson')
    if not os.path.exists(path):
        raise ValueError(f"Backup file not found: {path}")
    
    checksum = compute_checksum(path)
    prev = Variable.get('backup_checksum', default_var=None)
    if prev and prev == checksum:
        print("Warning: checksum same as previous!")
    Variable.set('backup_checksum', checksum)
    
    # Validation
    temp_db = 'temp_val_db'
    base_dir = os.path.dirname(path)
    cmd = f'mongorestore --uri="{MONGO_URI}" --db={temp_db} --drop {base_dir}'
    subprocess.run(cmd, shell=True, check=True)
    
    client = MongoClient(MONGO_URI)
    count = client[temp_db].get_collection(MONGO_COLLECTION).count_documents({})
    if count == 0:
        raise ValueError("Validation failed: no documents restored")
    client.drop_database(temp_db)
    print("Validation passed.")

cleanup_task = PythonOperator(
    task_id='cleanup_old_backups',
    python_callable=lambda: subprocess.run(f'rm -rf {BACKUP_DIR}/full_backup', shell=True),
    dag=dag
)

full_task = PythonOperator(task_id='full_backup', python_callable=full_backup, dag=dag)

# join_task = EmptyOperator(task_id='join', trigger_rule='one_success', dag=dag)

validate_task = PythonOperator(task_id='validate_backup', python_callable=validate_backup, dag=dag)

join_task = EmptyOperator(task_id='join', dag=dag)

#dag
full_task >> join_task >> validate_task >> cleanup_task