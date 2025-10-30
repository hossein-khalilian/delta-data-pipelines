from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from divar.utils.divar_fetcher import consume_and_fetch, transform
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
)

# RabbitMQ sensor task
rabbitmq_sensor = RabbitMQSensor(
    task_id="rabbitmq_sensor",
    queue_name=config["rabbitmq_queue"],
    batch_size=10,  # consume up to 10 messages per run
    timeout=600,  # maximum wait time in seconds
    dag=consumer_dag,
)


# consume_and_fetch task
def consume_and_fetch_wrapper(**context):
    # Pull messages from sensor via XCom
    messages = context["ti"].xcom_pull(task_ids="rabbitmq_sensor")
    if messages:
        consume_and_fetch()


consume_fetch_task = PythonOperator(
    task_id="consume_and_fetch",
    python_callable=consume_and_fetch_wrapper,
    provide_context=True,
    dag=consumer_dag,
)

# transform task
transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
    provide_context=True,
    dag=consumer_dag,
)

# store to mongo task
store_task = PythonOperator(
    task_id="store_to_mongo",
    python_callable=store_to_mongo,
    provide_context=True,
    dag=consumer_dag,
)

# DAG dependencies
rabbitmq_sensor >> consume_fetch_task >> transform_task >> store_task
