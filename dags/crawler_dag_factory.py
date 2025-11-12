import os
from datetime import datetime, timedelta
import importlib

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

from divar.utils.divar_crawler import extract_transform_urls, produce_to_rabbitmq

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "websites.yaml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

def load_function_with_path(path: str):
    """Dynamically import a parser function from a string path."""
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def extract_function(website_conf, **kwargs):
    extract_transform_urls(**kwargs)


def load_function(website_conf, **kwargs):
    pass


def create_crawler_dag(website_conf):
    dag_id = f"crawl_{website_conf['name']}"
    schedule = website_conf.get("crawler_schedule", "@daily")

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id=dag_id,
        description=f"Web crawler for {website_conf['name']}",
        schedule_interval=schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        concurrency=1,
        default_args=default_args,
        tags=["crawler", website_conf["name"]],
    ) as dag:

        # Producer DAG tasks
        extract_transform_task = PythonOperator(
            task_id="extract_transform_task",
            python_callable=extract_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        load_task = PythonOperator(
            task_id="load_task",
            python_callable=produce_to_rabbitmq,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        # Producer DAG graph
        extract_transform_task >> load_task

        # crawl_task = PythonOperator(
        #     task_id="crawl",
        #     python_callable=crawl_website,
        #     op_kwargs={
        #         "url": website_conf["url"],
        #         "parser": website_conf.get("parser", "default_parser"),
        #     },
        # )

    return dag


# --- Register each website as its own DAG ---
for website in config["websites"]:
    dag_id = f"crawl_{website['name']}"
    globals()[dag_id] = create_crawler_dag(website)
