import time
import httpx

# fetcher_function
def fetcher_function(messages):

    if not messages:
        print("No messages available from Sensor.")
        return []

    def fetch_all(messages):
        fetched = []
        with httpx.Client(verify=True) as client:
            for index, msg in enumerate(messages, start=1):
                print(index)
                
                url = msg["content_url"]
                
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                    fetched.append({"content_url": url, "data": resp.json()})
                    time.sleep(2)
                except Exception as e:
                    print(f"Fetch error {url}: {e}")
                    
        return fetched

    fetched_data = fetch_all(messages)
    if fetched_data:
        print(f"✅ Processed {len(fetched_data)} items")
    else:
        print("No data fetched from API.")
    return fetched_data

