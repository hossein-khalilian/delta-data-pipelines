import asyncio
import json
import httpx
from divar.utils.divar_transformer import transform_data
from utils.rabbitmq.rabbitmq_utils import iterate_queue_messages

from utils.config import config

DIVAR_API_URL = "https://api.divar.ir/v8/posts-v2/web/{token}"

async def consume_batch():
    with open("./dags/divar/utils/curl_commands/curl_command_01.txt") as f:
        curl_command = f.read()
    from curl2json.parser import parse_curl
    parsed = parse_curl(curl_command)
    parsed.pop("cookies", None)

    async with httpx.AsyncClient(headers=parsed.get("headers", {}), verify=True) as client:
        resp = await client.get("https://divar.ir")
        resp.raise_for_status()

        fetched = []
        async for message in iterate_queue_messages(config["rabbitmq_queue"], max_count = 40):
            async with message.process():
                token = json.loads(message.body.decode())
                try:
                    resp = await client.get(DIVAR_API_URL.format(token=token))
                    resp.raise_for_status()
                    fetched.append({"token": token, "data": resp.json()})
                    await asyncio.sleep(3)   
                except Exception as e:
                    print(f"Fetch error {token}: {e}")
            if len(fetched) >= 40:
                break

        return fetched

def fetch_function(**kwargs):
    fetched_data = asyncio.run(consume_batch())
    if fetched_data:
        kwargs["ti"].xcom_push(key="fetched_data", value=fetched_data)
        print(f"Processed: {len(fetched_data)} items")
    else:
        print("No messages in queue.")


def transform(**kwargs):
    fetched_data = kwargs["ti"].xcom_pull(
        key="fetched_data", task_ids="fetch_function"
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
