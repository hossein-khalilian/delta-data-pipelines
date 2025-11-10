from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from sheypoor_crawler import extract_transform_urls, produce_to_rabbitmq

# DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

producer_dag = DAG(
    "sheypoor_crawler",
    default_args=default_args,
    description="extract and filter urls from sheypoor",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1, 
)

# Producer DAG tasks
extract_transform_task = PythonOperator(
    task_id="extract_transform_task",
    python_callable=extract_transform_urls,
    provide_context=True,
    dag=producer_dag,
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=produce_to_rabbitmq,
    provide_context=True,
    dag=producer_dag,
)

# Producer DAG graph
extract_transform_task >> load_task
