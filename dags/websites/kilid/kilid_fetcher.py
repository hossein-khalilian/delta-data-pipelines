import httpx
import json
from typing import List, Dict, Any
import time 
def fetcher_function(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not messages:
            print("No messages available from Sensor.")
            return []
        
    # messages = messages[:5]  # 
        
    fetched_data = []

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for index, msg in enumerate(messages, start=1):
            url = msg.get("content_url")
            if not url:
                continue
            
            print(index)
            
            try:
                response = client.get(url)
                response.raise_for_status()

                fetched_item = {
                    "content_url": url,
                    "html_content": response.text,
                    "status_code": response.status_code,
                    "fetched_at": response.headers.get("date"),
                    "listingType": msg.get("listingType"),
                    "propertyType": msg.get("propertyType"),
                    "landuseType": msg.get("landuseType")
                }

                fetched_data.append(fetched_item)

            except httpx.RequestError as e:
                print(f"Request error for {url}: {e}")
                fetched_data.append({
                    "content_url": url,
                    "html_content": None,
                    "error": str(e),
                    "status_code": getattr(e, "status_code", None),
                    "listingType": msg.get("listingType"),
                    "propertyType": msg.get("propertyType"),
                    "landuseType": msg.get("landuseType")
                })
            except httpx.HTTPStatusError as e:
                print(f"HTTP error {e.response.status_code} for {url}")
                fetched_data.append({
                    "content_url": url,
                    "html_content": None,
                    "error": f"HTTP {e.response.status_code}",
                    "status_code": e.response.status_code,

                    "listingType": msg.get("listingType"),
                    "propertyType": msg.get("propertyType"),
                    "landuseType": msg.get("landuseType")
                })

            time.sleep(3)

    print(f"Fetcher completed: {len([f for f in fetched_data if f.get('html_content')])} successful out of {len(messages)}")
    return fetched_data