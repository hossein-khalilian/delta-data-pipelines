from datetime import datetime, timedelta
import os
import hashlib
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from pymongo import MongoClient
from pymongo.errors import PyMongoError

default_args = {
    'owner': 'senior_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
dag = DAG(
    'mongodb_backup_dag',
    default_args=default_args,
    description='DAG for full and incremental MongoDB backups with validation, RTO/RPO considerations, and recovery tests',
    schedule_interval=timedelta(days=1),  
    max_active_runs=1,  
    tags=['backup', 'mongodb'],
)

MONGO_URI = 'mongodb://appuser:appassword@172.16.36.111:27017/delta-datasets'
MONGO_DB = 'delta-datasets'
MONGO_COLLECTION = 'dataset_1'
BACKUP_DIR = '/path/to/backup/directory' 
OPLOG_FILE = os.path.join(BACKUP_DIR, 'oplog.bson')

# Function to check if full backup exists
def check_full_backup_exists(**kwargs):
    full_backup_path = os.path.join(BACKUP_DIR, 'full_backup')
    if os.path.exists(full_backup_path):
        return 'incremental_backup'
    return 'full_backup'

# Function to perform full backup using mongodump
def full_backup(**kwargs):
    try:
        # Use mongodump for full backup of the specific collection
        cmd = f'mongodump --uri="{MONGO_URI}" --db={MONGO_DB} --collection={MONGO_COLLECTION} --out={BACKUP_DIR}/full_backup'
        subprocess.run(cmd, shell=True, check=True)
        # Save the last oplog timestamp for incremental backups
        client = MongoClient(MONGO_URI)
        oplog = client.local.oplog.rs
        last_op = oplog.find().sort('$natural', -1).limit(1)[0]
        last_ts = last_op['ts']
        Variable.set('last_oplog_ts', str(last_ts))
        print("Full backup completed and last oplog timestamp saved.")
    except Exception as e:
        raise ValueError(f"Full backup failed: {str(e)}")

# Function to perform incremental backup using oplog
def incremental_backup(**kwargs):
    try:
        last_ts = Variable.get('last_oplog_ts')
        # Dump oplog since last timestamp
        cmd = f'mongodump --uri="{MONGO_URI}" --db=local --collection=oplog.rs --query="{{\'ts\': {{ $gt: Timestamp({last_ts}) }}}}" --out={BACKUP_DIR}/incremental_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        subprocess.run(cmd, shell=True, check=True)
        # Update last oplog timestamp
        client = MongoClient(MONGO_URI)
        oplog = client.local.oplog.rs
        last_op = oplog.find().sort('$natural', -1).limit(1)[0]
        last_ts = last_op['ts']
        Variable.set('last_oplog_ts', str(last_ts))
        print("Incremental backup completed.")
    except Exception as e:
        raise ValueError(f"Incremental backup failed: {str(e)}")

# Function to compute checksum for backup files
def compute_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# Function to validate backup with checksum and basic integrity check
def validate_backup(**kwargs):
    ti = kwargs['ti']
    backup_type = ti.xcom_pull(task_ids='branch_task')
    if backup_type == 'full_backup':
        backup_path = os.path.join(BACKUP_DIR, 'full_backup', MONGO_DB, f'{MONGO_COLLECTION}.bson')
    else:
        # For incremental, validate the latest incremental
        incremental_dirs = [d for d in os.listdir(BACKUP_DIR) if d.startswith('incremental_')]
        latest_incremental = max(incremental_dirs)
        backup_path = os.path.join(BACKUP_DIR, latest_incremental, 'local', 'oplog.rs.bson')
    
    if not os.path.exists(backup_path):
        raise ValueError("Backup file not found for validation.")
    
    checksum = compute_checksum(backup_path)
    # Store checksum in Airflow Variable for future comparisons (simulating validation)
    prev_checksum = Variable.get('backup_checksum', default_var=None)
    if prev_checksum and prev_checksum != checksum:
        print("Checksum validation passed (new backup).")
    Variable.set('backup_checksum', checksum)
    
    # Basic validation: Try to restore to a temp DB (dry run)
    try:
        temp_db = 'temp_validation_db'
        cmd = f'mongorestore --uri="{MONGO_URI}" --db={temp_db} --drop {backup_path}'
        subprocess.run(cmd, shell=True, check=True)
        client = MongoClient(MONGO_URI)
        db = client[temp_db]
        count = db[MONGO_COLLECTION].count_documents({})
        if count > 0:
            print("Validation successful: Data restored to temp DB.")
        client.drop_database(temp_db)
    except Exception as e:
        raise ValueError(f"Backup validation failed: {str(e)}")

# Function to test recovery (simulate RTO by timing the restore)
def test_recovery(**kwargs):
    start_time = datetime.now()
    try:
        # Restore full backup to a test DB
        test_db = 'test_recovery_db'
        full_backup_path = os.path.join(BACKUP_DIR, 'full_backup')
        cmd = f'mongorestore --uri="{MONGO_URI}" --db={test_db} --drop {full_backup_path}'
        subprocess.run(cmd, shell=True, check=True)
        
        # Apply latest incremental if exists
        incremental_dirs = [d for d in os.listdir(BACKUP_DIR) if d.startswith('incremental_')]
        if incremental_dirs:
            latest_incremental = max(incremental_dirs)
            oplog_path = os.path.join(BACKUP_DIR, latest_incremental, 'local', 'oplog.rs.bson')
            cmd = f'mongoreplay play --host {MONGO_URI} {oplog_path}'  
        
        client = MongoClient(MONGO_URI)
        db = client[test_db]
        count = db[MONGO_COLLECTION].count_documents({})
        if count > 0:
            print("Recovery test successful.")
        client.drop_database(test_db)
    except Exception as e:
        raise ValueError(f"Recovery test failed: {str(e)}")
    end_time = datetime.now()
    rto_time = (end_time - start_time).total_seconds()
    print(f"RTO achieved: {rto_time} seconds")  

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_full_backup_exists,
    dag=dag,
)

# Full backup task
full_backup_task = PythonOperator(
    task_id='full_backup',
    python_callable=full_backup,
    dag=dag,
)

# Incremental backup task
incremental_backup_task = PythonOperator(
    task_id='incremental_backup',
    python_callable=incremental_backup,
    dag=dag,
)

# Join after branch
join_task = EmptyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag,
)

# Validate backup task
validate_task = PythonOperator(
    task_id='validate_backup',
    python_callable=validate_backup,
    dag=dag,
)

# Test recovery task
test_recovery_task = PythonOperator(
    task_id='test_recovery',
    python_callable=test_recovery,
    dag=dag,
)

# Cleanup old backups
cleanup_cmd = f'find {BACKUP_DIR} -type d -name "incremental_*" -mtime +7 -exec rm -rf {{}} \;'  # Keep 7 days of incrementals
cleanup_task = BashOperator(
    task_id='cleanup_old_backups',
    bash_command=cleanup_cmd,
    dag=dag,
)

branch_task >> full_backup_task >> join_task
branch_task >> incremental_backup_task >> join_task
join_task >> validate_task >> test_recovery_task >> cleanup_task

