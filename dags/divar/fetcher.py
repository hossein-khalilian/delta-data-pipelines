from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from divar.utils.divar_fetcher import consume_and_fetch
# from utils.divar_fetcher import consume_and_fetch
from divar.utils.divar_fetcher import transform

from utils.kafka_utils import KafkaMessageSensor
from utils.mongodb_utils import store_to_mongo

# DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

consumer_dag = DAG(
    "divar_fetcher2",
    default_args=default_args,
    description="consume and fetch",
    schedule_interval="*/3 * * * *",
    catchup=False,
)

# Consumer DAG tasks 
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
    task_id="transform",
    python_callable=transform,
    provide_context=True,
    dag=consumer_dag,
)

store_task = PythonOperator(
    task_id="store_to_mongo",
    python_callable=store_to_mongo,
    provide_context=True,
    dag=consumer_dag,
)

#  Consumer DAG graph
kafka_sensor >> consume_fetch_task >> transform_task >> store_task
