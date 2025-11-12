import os
from datetime import datetime, timedelta
import importlib

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.config import config, RABBITMQ_HOST, RABBITMQ_PORT, MONGODB_URI
from utils.rabbitmq.rabbitmq_utils import RabbitMQSensor
from divar.utils.divar_fetcher import fetcher_function, transformer_function
from utils.mongodb_utils import store_to_mongo

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "websites.yaml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

def create_fetcher_dag(website_conf):
    dag_id = f"fetch_{website_conf['name']}"
    schedule = website_conf.get("fetcher_schedule", None)

    # if not schedule:
    #     return None 

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2024, 1, 1),
    }

    with DAG(
        dag_id=dag_id,
        description=f"Fetcher for {website_conf['name']}",
        schedule_interval=schedule,
        catchup=False,
        max_active_runs=1,
        concurrency=1,
        default_args=default_args,
        tags=["fetcher", website_conf["name"]],
    ) as dag:

        sensor = RabbitMQSensor(
            task_id="wait_for_messages",
            queue_name=website["queue_name"],
            batch_size=website.get("batch_size", 10),
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            timeout=600,
            dag=dag,
        )

        fetch = PythonOperator(
            task_id="fetch",
            python_callable=fetcher_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
            dag=dag,
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=transformer_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
            dag=dag,
        )

        load = PythonOperator(
            task_id="store_to_mongo",
            python_callable=store_to_mongo,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
            dag=dag,
        )

        sensor >> fetch >> transform >> load

    return dag


for website in config["websites"]:
    fetcher_dag = create_fetcher_dag(website)
    if fetcher_dag:
        globals()[f"fetch_{website['name']}"] = fetcher_dag