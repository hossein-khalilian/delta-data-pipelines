import asyncio
import importlib
import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.config import config
from utils.rabbitmq_utils import publish_messages
from utils.redis_utils import add_to_bloom_filter, check_bloom

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "websites.yaml")

with open(CONFIG_PATH, "r") as f:
    yaml_config = yaml.safe_load(f)

def load_function_with_path(path: str):
    """Dynamically import a parser function from a string path."""
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

def extract_transform_function(website_conf, **kwargs):
    func_path = website_conf["crawler"]
    crawler_func = load_function_with_path(func_path)
    all_urls = crawler_func()
    kwargs["ti"].xcom_push(key="extracted_urls", value=all_urls)

def load_function(website_conf, **kwargs):
    urls = kwargs["ti"].xcom_pull(
        key="extracted_urls", task_ids="extract_transform_task"
    )
    queue_name = f"{website_conf.get('name')}_{config.get('rabbitmq_urls_queue')}"
    bloom_key = f"{website_conf.get('name')}_{config.get('redis_bloom_filter')}"

    asyncio.run(publish_messages(queue_name, urls))

    content_urls = [url.get("content_url") for url in urls]
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(add_to_bloom_filter(bloom_key, content_urls))
    asyncio.run(check_bloom(bloom_key, content_urls))
    asyncio.run(add_to_bloom_filter(bloom_key, content_urls))

    print(f"✅Sent {len(urls)} URLs to RabbitMQ queue: {queue_name}")

def create_crawler_dag(website_conf):
    dag_id = f"{website_conf['name']}_crawler"
    schedule = website_conf.get("crawler_schedule")

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
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
            python_callable=extract_transform_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        load_task = PythonOperator(
            task_id="load_task",
            python_callable=load_function,
            provide_context=True,
            op_kwargs={"website_conf": website_conf},
        )

        # Producer DAG graph
        extract_transform_task >> load_task

    return dag

# Register each website as its own DAG
for website in yaml_config["websites"]:
    dag_id = f"{website['name']}_crawler"
    globals()[dag_id] = create_crawler_dag(website)
