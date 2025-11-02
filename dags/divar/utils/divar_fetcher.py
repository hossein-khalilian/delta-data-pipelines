import asyncio
import json
import re
import time
from datetime import datetime

import httpx
from aio_pika import IncomingMessage, connect_robust
from divar.utils.divar_transformer import transform_data
from utils.config import config

DIVAR_API_URL = "https://api.divar.ir/v8/posts-v2/web/{token}"


async def consume_batch():
    connection = await connect_robust(
        host=config["rabbitmq_host"],
        port=config["rabbitmq_port"],
        login=config["rabbitmq_user"],
        password=config["rabbitmq_pass"],
    )
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=40)
        queue = await channel.declare_queue(config["rabbitmq_queue"], durable=True)

        # Load curl
        with open("./dags/divar/utils/curl_commands/curl_command_01.txt") as f:
            curl_command = f.read()
        from curl2json.parser import parse_curl

        parsed = parse_curl(curl_command)
        parsed.pop("cookies", None)

        async with httpx.AsyncClient(
            headers=parsed.get("headers", {}), verify=True
        ) as client:
            resp = await client.get("https://divar.ir")
            resp.raise_for_status()

            fetched = []
            async for message in queue:
                async with message.process():
                    token = json.loads(message.body.decode())
                    try:
                        resp = await client.get(DIVAR_API_URL.format(token=token))
                        resp.raise_for_status()
                        fetched.append({"token": token, "data": resp.json()})
                        await asyncio.sleep(4.5)
                    except Exception as e:
                        print(f"Fetch error {token}: {e}")
                if len(fetched) >= 40:
                    break
            return fetched


def consume_and_fetch(**kwargs):
    fetched_data = asyncio.run(consume_batch())
    if fetched_data:
        kwargs["ti"].xcom_push(key="fetched_data", value=fetched_data)
        print(f"Processed: {len(fetched_data)} items")
    else:
        print("No messages in queue.")


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
            transformed_data.append(transformed)
        except Exception as e:
            print(f"Error converting JSON for {token}: {e}")
            continue

    kwargs["ti"].xcom_push(key="transformed_data", value=transformed_data)
