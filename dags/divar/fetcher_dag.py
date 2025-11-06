from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from divar.utils.divar_fetcher import fetcher_function, transformer_function
from utils.config import config
from utils.mongodb_utils import store_to_mongo
from utils.rabbitmq.rabbitmq_utils import RabbitMQSensor

# DAG default args
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
    description="Consume and fetch messages from RabbitMQ",
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1, 
)

# RabbitMQ sensor task
rabbitmq_sensor_task = RabbitMQSensor(
    task_id="rabbitmq_sensor_task",
    queue_name=config["rabbitmq_queue"],
    batch_size=40,  
    timeout=600,  
    dag=consumer_dag,
)

#fetcher_task
fetch_task = PythonOperator(
    task_id="fetch_task",
    python_callable=fetcher_function,
    provide_context=True,
    dag=consumer_dag,
)

# transformer task
transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transformer_function,
    provide_context=True,
    dag=consumer_dag,
)

# store to mongo task
load_task = PythonOperator(
    task_id="load_task",
    python_callable=store_to_mongo,
    provide_context=True,
    dag=consumer_dag,
)

# DAG dependencies
rabbitmq_sensor_task >> fetch_task >> transform_task >> load_task
