import json
import re
import time
from datetime import datetime

import httpx
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from divar.utils.transform import transform_json_to_doc
from utils.config import config


# ETL for fetch DAG
class KafkaMessageSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = config["kafka_bootstrap_servers"]
        self.topic = config["kafka_topic"]

    def poke(self, context):
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                # auto_offset_reset="latest",
                auto_offset_reset="earliest",
                group_id="divar_sensor_group",
                enable_auto_commit=False,
            )
            messages = consumer.poll(timeout_ms=5000)
            has_messages = any(len(records) > 0 for records in messages.values())
            consumer.close()

            if has_messages:
                print(f"✅ پیام جدیدی در تاپیک '{self.topic}' پیدا شد.")
            else:
                print(
                    f"⚠️ هیچ پیامی در تاپیک '{self.topic}' یافت نشد. منتظر پیام جدید می‌مانم..."
                )

            return has_messages

        except Exception as e:
            print(f"❌ خطا در بررسی پیام‌های Kafka: {e}")
            return False


def consume_and_fetch(**kwargs):
    consumer = KafkaConsumer(
        config["kafka_topic"],
        bootstrap_servers=config["kafka_bootstrap_servers"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="divar_consumer_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    messages = consumer.poll(timeout_ms=10000, max_records=20)
    consumer.commit()
    consumer.close()

    fetched_data = []
    client_params = {
        "verify": True,
        "headers": {"User-Agent": config["user_agent_default"]},
    }

    with httpx.Client(**client_params) as client:
        # دریافت کوکی‌ها
        try:
            resp = client.get("https://divar.ir")
            resp.raise_for_status()
            print("✅ Cookies دریافت شد")
        except Exception as e:
            print(f"❌ خطا در گرفتن cookies: {e}")
            return

        # پردازش توکن‌ها
        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                token = message.value
                url = config["divar_api_url"].format(token)
                try:
                    response = client.get(url)
                    response.raise_for_status()
                    data = response.json()
                    fetched_data.append({"token": token, "data": data})
                    print(f"دریافت شد: داده برای توکن {token}")
                    time.sleep(1.5)  # تأخیر 1.5 ثانیه‌ای بین درخواست‌ها
                except Exception as e:
                    print(f"خطا در دریافت محتوای {token}: {e}")
                    continue  # ادامه با توکن بعدی

    if fetched_data:
        kwargs["ti"].xcom_push(key="fetched_data", value=fetched_data)
        print(f"دریافت شد: {len(fetched_data)} توکن پردازش شد")
    else:
        print("هیچ پیامی در کافکا یافت نشد.")


def transform_data(**kwargs):
    fetched_data = kwargs["ti"].xcom_pull(
        key="fetched_data", task_ids="consume_and_fetch"
    )
    if not fetched_data:
        print("هیچ داده‌ای برای تبدیل وجود ندارد.")
        return

    transformed_data = []
    for item in fetched_data:
        token = item["token"]
        data = item["data"]
        try:
            transformed = transform_json_to_doc(data)
            transformed["post_token"] = token
            transformed["crawl_timestamp"] = datetime.utcnow().isoformat()
            transformed_data.append(transformed)
            print(f"تبدیل شد: داده برای توکن {token}")
        except Exception as e:
            print(f"خطا در تبدیل JSON برای {token}: {e}")
            continue

    kwargs["ti"].xcom_push(key="transformed_data", value=transformed_data)


def store_to_mongo(**kwargs):
    transformed_data = kwargs["ti"].xcom_pull(
        key="transformed_data", task_ids="transform_data"
    )
    if not transformed_data:
        print("هیچ داده‌ای برای ذخیره در MongoDB وجود ندارد.")
        return

    client = MongoClient(config["mongo_uri"])
    db = client[config["mongo_db"]]
    collection = db[config["mongo_collection"]]
    try:
        collection.create_index("post_token", unique=True)
        for transformed in transformed_data:
            try:
                collection.insert_one(transformed)
                print(
                    f"ذخیره شد: داده برای توکن {transformed['post_token']} در MongoDB"
                )
            except DuplicateKeyError:
                print(f"تکراری: توکن {transformed['post_token']} قبلاً ذخیره شده است.")
            except Exception as e:
                print(f"خطا در ذخیره {transformed['post_token']}: {e}")
    finally:
        client.close()
