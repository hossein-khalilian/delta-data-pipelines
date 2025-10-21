from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from divar.utils.crawler_utils import extract_tokens, filter_tokens, produce_to_kafka

# DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

producer_dag = DAG(
    "divar_crawler",
    default_args=default_args,
    description="استخراج 100 توکن دیوار، فیلتر با بلوم، و ارسال به کافکا هر 5 دقیقه",
    schedule_interval="*/5 * * * *",
    catchup=False,
)

# --- تسک‌های DAG تولیدکننده ---
extract_task = PythonOperator(
    task_id="extract_tokens",
    python_callable=extract_tokens,
    provide_context=True,
    dag=producer_dag,
)

filter_task = PythonOperator(
    task_id="filter_tokens",
    python_callable=filter_tokens,
    provide_context=True,
    dag=producer_dag,
)

produce_task = PythonOperator(
    task_id="produce_to_kafka",
    python_callable=produce_to_kafka,
    provide_context=True,
    dag=producer_dag,
)

#  گراف DAG تولیدکننده
extract_task >> filter_task >> produce_task
