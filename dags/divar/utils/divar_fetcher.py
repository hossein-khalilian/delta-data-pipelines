import asyncio
import httpx
from divar.utils.divar_transformer import transform_data

DIVAR_API_URL = "https://api.divar.ir/v8/posts-v2/web/{token}"

# fetcher_function
def fetcher_function(**kwargs):
    fetched_messages = kwargs["ti"].xcom_pull(
        key="return_value", task_ids="rabbitmq_sensor_task"
    )

    if not fetched_messages:
        print("No messages available from Sensor.")
        return

    async def fetch_all(messages):
        async with httpx.AsyncClient(verify=True) as client:
            fetched = []
            for index, msg in enumerate(messages, start=1):
                print(f"{index}")
                
                url = msg["content_url"]
                
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    fetched.append({"content_url": url, "data": resp.json()})
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"Fetch error {url}: {e}")
            return fetched

    fetched_data = asyncio.run(fetch_all(fetched_messages))
    if fetched_data:
        kwargs["ti"].xcom_push(key="fetched_data", value=fetched_data)
        print(f"✅ Processed {len(fetched_data)} items")
    else:
        print("No data fetched from API.")

def transformer_function(**kwargs):
    fetched_data = kwargs["ti"].xcom_pull(
        key="fetched_data", task_ids="fetch_task"
    )
    if not fetched_data:
        print("⚠️No data available for transformation.")
        return

    transformed_data = []
    for item in fetched_data:
        url = item["content_url"]
        data = item["data"]
        try:
            transformed = transform_data(data)
            transformed["content_url"] = url
            transformed_data.append(transformed)
        except Exception as e:
            print(f"Error converting JSON for {url}: {e}")
            continue
        
    print(f"✅ Transformed {len(transformed_data)} items.")

    kwargs["ti"].xcom_push(key="transform_data", value=transformed_data)
