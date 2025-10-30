from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from divar.utils.divar_fetcher import consume_and_fetch, transform
from utils.config import config
from utils.mongodb_utils import store_to_mongo
from utils.rabbitmq.rabbitmq_utils import RabbitMQSensor
from utils.rabbitmq.rabbitmq_utils import RabbitMQSensorTrigger

# DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

consumer_dag = DAG(
    "divar_fetcher",
    default_args=default_args,
    description="consume and fetch",
    schedule_interval="*/3 * * * *",
    catchup=False,
)

# Consumer DAG tasks
rabbitmq_sensor = RabbitMQSensor(
    task_id="rabbitmq_sensor",
    queue_name=config["rabbitmq_queue"],
    # poke_interval=40,
    timeout=600,
    # mode="poke", 
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
rabbitmq_sensor >> consume_fetch_task >> transform_task >> store_task
