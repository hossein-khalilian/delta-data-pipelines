from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import os

import redis
import yaml
from pymongo import MongoClient
from utils.config import config
from web_scraping.utils.redis_utils import add_to_bloom_filter

CONFIG_PATH = "/opt/airflow/dags/web_scraping/websites.yaml"

with open(CONFIG_PATH, "r") as f:
    yaml_config = yaml.safe_load(f)

site_names = [site["name"] for site in yaml_config["websites"]]

MONGO_URI = config["mongo_uri"]
MONGO_DB = config["mongo_db"]
MONGO_COLLECTION = config["mongo_collection"]

REDIS_URL = config["redis_url"]
REDIS_BLOOM_FILTER = config["redis_bloom_filter"]

websites_mongo_collection = [f"{name}-{MONGO_COLLECTION}" for name in site_names]
websites_bloom_filter = [f"{name}_{REDIS_BLOOM_FILTER}" for name in site_names]

BATCH_SIZE = 1000

# Functions 
def ensure_bloom_exists(r, name):
    if r.exists(name) == 0:
        raise Exception(f"Bloom filter '{name}' does NOT exist in Redis!")
    print(f"Bloom filter '{name}' exists.")

async def process_collection_and_bloom(collection_name, bloom_name):
    print(f"Processing MongoDB Collection: {collection_name} => Bloom Filter: {bloom_name}")

    mongo = MongoClient(MONGO_URI)
    collection = mongo[MONGO_DB][collection_name]

    r = redis.from_url(REDIS_URL)
    ensure_bloom_exists(r, bloom_name)

    cursor = collection.find(
        {"content_url": {"$exists": True, "$ne": None}},
        {"content_url": 1, "_id": 0},
    )

    batch = []
    inserted = 0
    duplicates = 0

    for doc in cursor:
        url = doc.get("content_url")
        if not url:
            continue

        batch.append(url)

        if len(batch) >= BATCH_SIZE:
            result = await add_to_bloom_filter(bloom_name, batch)
            inserted += sum(1 for x in result if x == 1)
            duplicates += sum(1 for x in result if x == 0)
            batch = []

    if batch:
        result = await add_to_bloom_filter(bloom_name, batch)
        inserted += sum(1 for x in result if x == 1)
        duplicates += sum(1 for x in result if x == 0)

    print(f"Batch Inserted URLs : {inserted}")
    print(f"Batch Duplicate URLs : {duplicates}\n")


async def main_async():
    for mongo_col, bloom_filter in zip(websites_mongo_collection, websites_bloom_filter):
        await process_collection_and_bloom(mongo_col, bloom_filter)

def run_scraping_task():
    asyncio.run(main_async())

# DAG 

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 16),
}

dag = DAG(
    dag_id="add-urls-to-BF",
    default_args=default_args,
    description="add MongoDB URLs to BF",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

process_task = PythonOperator(
    task_id="extract_and_load",
    python_callable=run_scraping_task,
    dag=dag,
)
