from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from divar.utils.fetcher_utils import (
    KafkaMessageSensor,
    consume_and_fetch,
    store_to_mongo,
    transform_data,
)

# DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

consumer_dag = DAG(
    "divar_fetcher",
    default_args=default_args,
    description="مصرف یک توکن از کافکا، دریافت، تبدیل و ذخیره در MongoDB هر 5 دقیقه",
    schedule_interval="*/5 * * * *",
    catchup=False,
)

# --- تسک‌های DAG مصرف‌کننده ---
kafka_sensor = KafkaMessageSensor(
    task_id="kafka_message_sensor",
    poke_interval=60,
    timeout=600,
    dag=consumer_dag,
)

consume_fetch_task = PythonOperator(
    task_id="consume_and_fetch",
    python_callable=consume_and_fetch,
    provide_context=True,
    dag=consumer_dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=consumer_dag,
)

store_task = PythonOperator(
    task_id="store_to_mongo",
    python_callable=store_to_mongo,
    provide_context=True,
    dag=consumer_dag,
)

#  گراف DAG مصرف‌کننده
kafka_sensor >> consume_fetch_task >> transform_task >> store_task
