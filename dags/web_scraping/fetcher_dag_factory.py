import importlib
import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.config import config
from web_scraping.utils.mongodb_utils import store_to_mongo
from web_scraping.utils.rabbitmq_utils import RabbitMQSensor

# Load YAML config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "websites.yaml")
with open(CONFIG_PATH, "r") as f:
    yaml_config = yaml.safe_load(f)


def load_function_with_path(path: str):
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def extract_function(website_conf, **kwargs):
    messages = kwargs["ti"].xcom_pull(task_ids="sensor_task", key="return_value")
    fetcher_func = load_function_with_path(website_conf["fetcher"])
    fetched = fetcher_func(messages)
    kwargs["ti"].xcom_push(key="fetched_data", value=fetched)


def transform_function(website_conf, **kwargs):
    fetched = kwargs["ti"].xcom_pull(task_ids="extract_task", key="fetched_data")
    transformer_func = load_function_with_path(website_conf["transformer"])
    transformed = transformer_func(fetched)
    kwargs["ti"].xcom_push(key="transform_data", value=transformed)


def load_function(website_conf, **kwargs):
    transformed_data = kwargs["ti"].xcom_pull(
        key="transform_data", task_ids="transform_task"
    )
    collection_name = f"{website_conf['name']}-{config.get('mongo_collection')}"
    store_to_mongo(transformed_data, collection_name=collection_name)


def create_fetcher_dag(website_conf):
    dag_id = f"{website_conf['name']}-fetcher"
    schedule = website_conf.get("fetcher_schedule", None)

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
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

        queue_name = f"{website_conf['name']}_{config.get('rabbitmq_urls_queue')}"

        sensor_task = RabbitMQSensor(
            task_id="sensor_task",
            queue_name=queue_name,
            batch_size=website_conf.get("batch_size", 50),
            timeout=60,
        )

        extract_task = PythonOperator(
            task_id="extract_task",
            python_callable=extract_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        transform_task = PythonOperator(
            task_id="transform_task",
            python_callable=transform_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        load_task = PythonOperator(
            task_id="load_task",
            python_callable=load_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        sensor_task >> extract_task >> transform_task >> load_task

    return dag


# Register each website as its own DAG
for website in yaml_config["websites"]:
    dag_id = f"{website['name']}-fetcher"
    globals()[dag_id] = create_fetcher_dag(website)
