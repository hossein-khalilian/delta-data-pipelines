from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from backup.utils.utils_of_backup import (
    full_backup_function,
    restore_function,
    validate_function,
    cleanup_function,
)

# DAG 
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dag = DAG(
    "mongodb_full_backup",
    default_args=default_args,
    schedule_interval="@weekly",
    max_active_runs=1,
    tags=["mongodb", "backup", "minio"],
)

# OPERATORS 
full_backup_task = PythonOperator(
    task_id="backup_task",
    python_callable=full_backup_function,
    dag=dag,
)

restore_task = PythonOperator(
    task_id="restore_task",
    python_callable=restore_function,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_task",
    python_callable=validate_function,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup_function,
    dag=dag,
)

full_backup_task >> restore_task >> validate_task >> cleanup_task
