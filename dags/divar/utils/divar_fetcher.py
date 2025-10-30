import json
import re
import time
from datetime import datetime
import httpx

from kafka import KafkaConsumer, KafkaProducer
from divar.utils.divar_transformer import transform_data

from utils.config import config

DIVAR_API_URL = "https://api.divar.ir/v8/posts-v2/web/{token}"

def consume_and_fetch(**kwargs):
    consumer = KafkaConsumer(
        config["kafka_topic"],
        bootstrap_servers=config["kafka_bootstrap_servers"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="divar_consumer_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    messages = consumer.poll(timeout_ms=10000, max_records=40)
    consumer.commit()
    consumer.close()

    fetched_data = []
    client_params = {
        "verify": True,
        "headers": {"User-Agent": config["user_agent_default"]},
    }

    with httpx.Client(**client_params) as client:
        # receive cookies
        try:
            resp = client.get("https://divar.ir")
            resp.raise_for_status()
            print("✅ Cookies received")
        except Exception as e:
            print(f"❌ Error while getting cookies: {e}")
            return

        # Process tokens
        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                token = message.value
                url = DIVAR_API_URL.format(token=token)
                try:
                    response = client.get(url)
                    response.raise_for_status()
                    data = response.json()
                    fetched_data.append({"token": token, "data": data})
                    # print(f"Received: data for token {token}")
                    time.sleep(1.5)
                except Exception as e:
                    print(f"Error while fetching content for {token}: {e}")
                    continue

    if fetched_data:
        kwargs["ti"].xcom_push(key="fetched_data", value=fetched_data)
        print(f"Received: {len(fetched_data)} tokens processed")
    else:
        print("⚠️No messages found in Kafka.")


def transform(**kwargs):
    fetched_data = kwargs["ti"].xcom_pull(
        key="fetched_data", task_ids="consume_and_fetch"
    )
    if not fetched_data:
        print("⚠️No data available for transformation.")
        return

    transformed_data = []
    for item in fetched_data:
        token = item["token"]
        data = item["data"]
        try:
            transformed = transform_data(data)
            transformed["post_token"] = token
            transformed["crawl_timestamp"] = datetime.utcnow().isoformat()
            transformed_data.append(transformed)
        except Exception as e:
            print(f"Error converting JSON for {token}: {e}")
            continue

    kwargs["ti"].xcom_push(key="transformed_data", value=transformed_data)
