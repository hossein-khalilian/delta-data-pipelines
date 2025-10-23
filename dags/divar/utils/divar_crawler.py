import json
import time
from datetime import datetime, timedelta

import httpx
import redis
from curl2json.parser import parse_curl
from kafka import KafkaConsumer, KafkaProducer

from utils.config import config


# ETL for crawler DAG
def extract_tokens(**kwargs):
    BLOOM_KEY = config["redis_bloom_filter"]
    print(config["redis_host"])
    print(config["redis_port"])
    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"])

    # Bloom filter
    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command("BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000)
            print(f"✅ Bloom filter named {BLOOM_KEY} has been created")
        except Exception as e:
            print(f"⚠️ Error while creating Bloom filter: {e}")
    else:
        print(f"✅ Bloom filter named {BLOOM_KEY} already exists")

    # curl_command
    try:
        with open(
            "./dags/divar/utils/curl_commands/curl_command_01.txt",
            "r",
            encoding="utf-8",
        ) as file:
            curl_command = file.read()
        print("✅ File curl_command_01.txt was read successfully")
    except Exception as e:
        print(f"❌ Error reading file curl_command_01.txt: {e}")
        return

    parsed_curl = parse_curl(curl_command)
    parsed_curl.pop("cookies", None)

    client_params = {
        "verify": True,
        "headers": parsed_curl.pop("headers", {}),
    }

    all_tokens = set()
    max_pages = 100

    with httpx.Client(**client_params) as client:
        # GET for get Cookies
        try:
            resp = client.get("https://divar.ir")
            resp.raise_for_status()
            print("✅ Cookies received")
        except Exception as e:
            print(f"❌ Error fetching cookies: {e}")
            return

        curl_data = json.loads(parsed_curl.get("data"))

        for page in range(max_pages):
            try:
                # udate pagination_data
                curl_data["pagination_data"]["page"] = page
                curl_data["pagination_data"]["layer_page"] = 0
                parsed_curl["data"] = json.dumps(curl_data)

                # POST request
                response = client.request(
                    method=parsed_curl.get("method", "POST"),
                    url=parsed_curl["url"],
                    headers=parsed_curl.get("headers", {}),
                    content=parsed_curl.get("data"),
                    params=parsed_curl.get("params"),
                )
                response.raise_for_status()
                result = response.json()

                # Extract tokens
                widgets = result.get("list_widgets", []) or []
                tokens = [
                    w.get("data", {}).get("token")
                    for w in widgets
                    if w.get("data", {}).get("token")
                ]
                if not tokens:
                    print(f"⛔️ Page {page}: No tokens found, stopping.")
                    break

                # for t in tokens:
                #     print(f"🔹 Token found: {t}")

                # print(f"📄 Page {page}: {result.get('list_widgets')[0].get('data').get('title')}")
                print(f"Page: {page}")
                print(f"📊 Number of ads: {len(widgets)}")

                # for w in widgets:
                #     print(f"🔹 Widget found: {w}")

                # Check for duplicate tokens
                duplicate_count, new_tokens = 0, []
                for token in tokens:
                    exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, token)
                    if exists:
                        duplicate_count += 1
                    else:
                        new_tokens.append(token)
                        rdb.execute_command("BF.ADD", BLOOM_KEY, token)

                all_tokens.update(new_tokens)
                ratio = duplicate_count / len(tokens) if tokens else 1
                print(
                    f"📊 {duplicate_count}/{len(tokens)} duplicates ({ratio:.0%})"
                )

                if ratio >= 0.3:
                    print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                    break

                # update pagination_data
                pagination_info = result.get("pagination", {}) or {}
                curl_data["pagination_data"] = pagination_info.get(
                    "data", curl_data["pagination_data"]
                )

                time.sleep(1.5)

            except Exception as e:
                print(f"❌ Error requesting page {page}: {e}")
                break

    kwargs["ti"].xcom_push(key="extracted_tokens", value=list(all_tokens))
    print(f"✅ Extraction completed — {len(all_tokens)} new tokens pushed to XCom.")


def filter_tokens(**kwargs):
    tokens = (
        kwargs["ti"].xcom_pull(key="extracted_tokens", task_ids="extract_tokens") or []
    )
    if not tokens:
        print("No tokens available for filtering.")
        kwargs["ti"].xcom_push(key="filtered_tokens", value=[])
        return

    # r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    # new_tokens = []
    # for token in tokens:
    #     exists = r.execute_command("BF.EXISTS", REDIS_BLOOM_FILTER, token)
    #     if not exists:
    #         r.execute_command("BF.ADD", REDIS_BLOOM_FILTER, token)
    #         new_tokens.append(token)

    kwargs["ti"].xcom_push(key="filtered_tokens", value=tokens)
    print(f"Transferred: {len(tokens)} tokens to XCom")


def produce_to_kafka(**kwargs):
    tokens = kwargs["ti"].xcom_pull(key="filtered_tokens", task_ids="filter_tokens")
    if not tokens:
        print("No new tokens to send to Kafka.")
        return

    producer = KafkaProducer(
        bootstrap_servers=config["kafka_bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for token in tokens:
        producer.send(config["kafka_topic"], token)
    producer.flush()
    print(f"Sent: {len(tokens)} tokens to Kafka")