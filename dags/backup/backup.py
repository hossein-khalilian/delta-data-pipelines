from datetime import datetime, timedelta
import os
import hashlib
import subprocess
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.timestamp import Timestamp
from utils.config import config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
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
# MONGO_COLLECTION = config["mongo_collection"]
MONGO_COLLECTION = "divar-dataset_1"

BACKUP_DIR = './git/delta-data-pipelines/dags/backup/mongodb_backup'
os.makedirs(BACKUP_DIR, exist_ok=True)

mongodump_bin = "mongodump"

def check_full_backup_exists(**kwargs):
    full_backup_path = os.path.join(BACKUP_DIR, 'full_backup')
    return 'incremental_backup' if os.path.exists(full_backup_path) else 'full_backup'

def full_backup(**kwargs):
    cmd = f'{mongodump_bin} --uri="{MONGO_URI}" --db={MONGO_DB} --collection={MONGO_COLLECTION} --out={BACKUP_DIR}/full_backup'
    subprocess.run(cmd, shell=True, check=True)
    
    client = MongoClient(MONGO_URI)
    last_op = client.local.oplog.rs.find().sort('$natural', -1).limit(1)[0]
    ts_dict = {'t': last_op['ts'].time, 'i': last_op['ts'].inc}
    Variable.set('last_oplog_ts', json.dumps(ts_dict))
    print("Full backup done.")

def incremental_backup(**kwargs):
    ts_str = Variable.get('last_oplog_ts')
    if not ts_str:
        raise ValueError("No last_oplog_ts → first run full backup")
    ts_dict = json.loads(ts_str)
    
    query = f'{{ "ts": {{ "$gt": Timestamp({ts_dict["t"]}, {ts_dict["i"]}) }} }}'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    inc_dir = os.path.join(BACKUP_DIR, f'incremental_{timestamp}')
    
    cmd = f'{mongodump_bin} --uri="{MONGO_URI}" --db=local --collection=oplog.rs --query=\'{query}\' --out={inc_dir}'
    subprocess.run(cmd, shell=True, check=True)
    
    #  timestamp
    client = MongoClient(MONGO_URI)
    last_op = client.local.oplog.rs.find().sort('$natural', -1).limit(1)[0]
    ts_dict = {'t': last_op['ts'].time, 'i': last_op['ts'].inc}
    Variable.set('last_oplog_ts', json.dumps(ts_dict))
    print(f"Incremental done → {inc_dir}")

def compute_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def validate_backup(**kwargs):
    ti = kwargs['ti']
    branch_result = ti.xcom_pull(task_ids='branch_task')
    
    if branch_result == 'full_backup':
        path = os.path.join(BACKUP_DIR, 'full_backup', MONGO_DB, f'{MONGO_COLLECTION}.bson')
    else:
        inc_dirs = [d for d in os.listdir(BACKUP_DIR) if d.startswith('incremental_')]
        if not inc_dirs:
            raise ValueError("No incremental found for validation")
        latest = max(inc_dirs)
        path = os.path.join(BACKUP_DIR, latest, 'local', 'oplog.rs.bson')
    
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
    count = client[temp_db].get_collection(MONGO_COLLECTION if branch_result == 'full_backup' else 'oplog.rs').count_documents({})
    if count == 0:
        raise ValueError("Validation failed: no documents restored")
    client.drop_database(temp_db)
    print("Validation passed.")

#tasks
branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_full_backup_exists,
    dag=dag,
)

full_task = PythonOperator(task_id='full_backup', python_callable=full_backup, dag=dag)
inc_task = PythonOperator(task_id='incremental_backup', python_callable=incremental_backup, dag=dag)

join_task = EmptyOperator(task_id='join', trigger_rule='one_success', dag=dag)

validate_task = PythonOperator(task_id='validate_backup', python_callable=validate_backup, dag=dag)

cleanup_task = BashOperator(
    task_id='cleanup_old_backups',
    bash_command=f'find {BACKUP_DIR} -type d -name "incremental_*" -mtime +7 -exec rm -rf {{}} \;',
    dag=dag,
)

#dag
branch_task >> [full_task, inc_task]
[full_task, inc_task] >> join_task >> validate_task >> cleanup_task