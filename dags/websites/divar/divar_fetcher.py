import asyncio
import httpx

# fetcher_function
def fetcher_function(messages):

    if not messages:
        print("No messages available from Sensor.")
        return []

    async def fetch_all(messages):
        async with httpx.AsyncClient(verify=True) as client:
            fetched = []
            for index, msg in enumerate(messages, start=1):
                print(index)
                
                url = msg["content_url"]
                
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    fetched.append({"content_url": url, "data": resp.json()})
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Fetch error {url}: {e}")
            return fetched

    fetched_data = asyncio.run(fetch_all(messages))
    if fetched_data:
        print(f"✅ Processed {len(fetched_data)} items")
    else:
        print("No data fetched from API.")
    return fetched_data

