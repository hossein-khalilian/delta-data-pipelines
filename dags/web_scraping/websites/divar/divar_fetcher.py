import time
import httpx

def fetcher_function(messages):
    if not messages:
        print("No messages available from Sensor.")
        return []

    fetched = []

    with httpx.Client(verify=True) as client:
        for index, msg in enumerate(messages, 1):
            print(index)
            url = msg["content_url"]
            try:
                resp = client.get(url)
                resp.raise_for_status()
                fetched.append({"content_url": url, "data": resp.json()})
                time.sleep(2)
            except Exception as e:
                print(f"Fetch error {url}: {e}")

    print(
        f"✅ Processed {len(fetched)} items" if fetched else "No data fetched from API."
    )
    return fetched
