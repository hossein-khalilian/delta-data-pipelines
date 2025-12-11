import asyncio
import json
import time

import httpx
import redis
from curl2json.parser import parse_curl

from utils.config import config


# ETL for crawler DAG
def extract_transform_urls():
    BLOOM_KEY = f"divar_{config.get('redis_bloom_filter')}"

    rdb = redis.from_url(config["redis_url"])

    # Bloom filter
    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command(
                "BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000, "EXPANSION", 2
            )
            print(f"✅ Bloom filter '{BLOOM_KEY}' created")
        except Exception as e:
            print(f"⚠️ Error while creating Bloom filter: {e}")
    else:
        print(f"✅ Using Bloom Filter: {BLOOM_KEY}")

    # curl_command
    try:
        with open(
            "./dags/websites/divar/curl_commands/curl_command_01.txt",
            "r",
            encoding="utf-8",
        ) as file:
            curl_command = file.read()
    except Exception as e:
        print(f"❌ Error reading file curl_command_01.txt: {e}")
        return

    parsed_curl = parse_curl(curl_command)
    parsed_curl.pop("cookies", None)

    try:
        with open(
            "./dags/websites/divar/curl_commands/first_request.txt",
            "r",
            encoding="utf-8",
        ) as file:
            first_request_curl = file.read()
    except Exception as e:
        print(f"❌ Error reading first_request_curl.txt: {e}")
        return

    parsed_first = parse_curl(first_request_curl)
    cookies = parsed_first.pop("cookies", {})

    client_params = {
        "verify": True,
        "headers": parsed_curl.pop("headers", {}),
        "cookies": cookies,
    }

    all_urls = []
    max_pages = 50
    stop_condition = False

    with httpx.Client(**client_params) as client:

        # print(client.headers)

        # GET for get Cookies
        try:
            print("✅ Cookies received")
        except Exception as e:
            print(f"❌ Error fetching cookies: {e}")
            # return
            raise RuntimeError("Task failed because cookies could not be fetched")

        curl_data = json.loads(parsed_curl.get("data"))
        
        pro_ads_total = 0
        
        for page in range(max_pages):
            try:
                # update pagination
                curl_data["pagination_data"]["page"] = page
                curl_data["pagination_data"]["layer_page"] = 0
                parsed_curl["data"] = json.dumps(curl_data)

                print(f" =========== Page: {page} =========== ")

                # POST request
                response = client.request(
                    method=parsed_curl.get("method", "POST"),
                    url=parsed_curl["url"],
                    headers=parsed_curl.get("headers", {}),
                    content=parsed_curl.get("data"),
                    params=parsed_curl.get("params"),
                )
                response.raise_for_status()

                # print("=== Response Headers ===")
                # print(response.headers)

                result = response.json()

                # Extract tokens
                widgets = result.get("list_widgets", []) or []

                # pro_ads = {"نردبان شده", "پله شده"}
                
                # pro_widgets = [
                #     w for w in widgets
                #     if w.get("data", {}).get("red_text") in pro_ads]
                
                # pro_ads_total += len(pro_widgets)

                # # normal ads
                # filtered_widgets = [
                #     w for w in widgets
                #     if w.get("data", {}).get("red_text") not in pro_ads
                # ]

                # print(f"📊 pro ads : {len(pro_widgets)}")
                # print(f"📊 Valid ads: {len(filtered_widgets)}")

                # tokens = [
                #     w.get("data", {}).get("token")
                #     for w in filtered_widgets
                #     if w.get("data", {}).get("token")
                # ]
                # if not tokens:
                #     print(f"⛔️ Page {page}: No tokens found, stopping.")
                #     break

                # # for t in tokens:
                # #     print(f"🔹 Token found: {t}")
                # # print(f"📄 Page {page}: {result.get('list_widgets')[0].get('data').get('title')}")

                # print(f"📊 Total ads: {len(widgets)}")

                # # Check for duplicate tokens
                # duplicate_count, new_tokens, duplicate_tokens = 0, [], []
                # for token in tokens:
                #     content_url = f"https://api.divar.ir/v8/posts-v2/web/{token}"

                #     exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, content_url)
                #     if exists:
                #         duplicate_count += 1
                #         duplicate_tokens.append(content_url)
                #     else:
                #         new_tokens.append(content_url)

                # ratio = duplicate_count / len(tokens) if tokens else 1
                # print(f"📌 {duplicate_count}/{len(tokens)} duplicates ({ratio:.0%})")

                # if ratio >= 0.5:
                #     print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                #     stop_condition = True

                # if not stop_condition:
                #     all_urls_to_push = new_tokens + duplicate_tokens
                # else:
                #     all_urls_to_push = new_tokens

                # new_urls = [{"content_url": url} for url in all_urls_to_push]
                # all_urls.extend(new_urls)

                # if stop_condition:
                #     break
                pro_ads = {"نردبان شده", "پله شده"}

                # Split widgets
                pro_widgets = [w for w in widgets if w.get("data", {}).get("red_text") in pro_ads]
                normal_widgets = [w for w in widgets if w.get("data", {}).get("red_text") not in pro_ads]

                pro_ads_total += len(pro_widgets)

                print(f"📊 Pro ads : {len(pro_widgets)}")
                print(f"📊 Normal ads: {len(normal_widgets)}")

                # Extract tokens
                pro_tokens = [
                    w.get("data", {}).get("token")
                    for w in pro_widgets
                    if w.get("data", {}).get("token")
                ]

                normal_tokens = [
                    w.get("data", {}).get("token")
                    for w in normal_widgets
                    if w.get("data", {}).get("token")
                ]

                if not normal_tokens:
                    print(f"⛔️ Page {page}: No normal tokens found, stopping.")
                    break

                # Build URLs
                normal_urls = [f"https://api.divar.ir/v8/posts-v2/web/{t}" for t in normal_tokens]
                pro_urls = [f"https://api.divar.ir/v8/posts-v2/web/{t}" for t in pro_tokens]

                # Check duplicate ratio ONLY on normal URLs
                duplicate_count = sum(
                    rdb.execute_command("BF.EXISTS", BLOOM_KEY, url)
                    for url in normal_urls
                )

                ratio = duplicate_count / len(normal_urls)
                print(f"📌 {duplicate_count}/{len(normal_urls)} duplicates ({ratio:.0%})")

                # Stop based on normal ads only
                if ratio >= 0.5:
                    print(f"🛑 More than 50% duplicates — stopping.")
                    stop_condition = True

                # All URLs must be pushed (normal + pro)
                all_page_urls = normal_urls + pro_urls

                new_urls = [{"content_url": url} for url in all_page_urls]
                all_urls.extend(new_urls)

                if stop_condition:
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
            
    print(f"📢 Total pro ads in crawl: {pro_ads_total}")

    print(f"✅ Extraction completed — {len(all_urls)} new urls extracted")
    return list(all_urls)
