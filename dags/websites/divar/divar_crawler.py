import asyncio
import json
import time
import httpx
import redis
from curl2json.parser import parse_curl
from utils.config import config

# ETL for crawler DAG
def extract_transform_urls():
    BLOOM_KEY = f"diver_{config.get('redis_bloom_filter')}"

    print(f"Using Bloom Filter: {BLOOM_KEY}")
    print(config["redis_host"])
    print(config["redis_port"])
    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"])

    # Bloom filter
    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command(
                "BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000, "EXPANSION", 2
            )
            print(f"✅ Bloom filter named {BLOOM_KEY} has been created")
        except Exception as e:
            print(f"⚠️ Error while creating Bloom filter: {e}")
    else:
        print(f"✅ Bloom filter named {BLOOM_KEY} already exists")

    # curl_command
    try:
        with open(
            "./dags/websites/divar/curl_commands/curl_command_01.txt",
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

    all_urls = []
    max_pages = 50
    stop_condition = False

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
                duplicate_count, new_tokens, duplicate_tokens = 0, [], []
                for token in tokens:
                    exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, token)
                    if exists:
                        duplicate_count += 1
                        duplicate_tokens.append(token)
                    else:
                        new_tokens.append(token)
                        rdb.execute_command("BF.ADD", BLOOM_KEY, token)

                ratio = duplicate_count / len(tokens) if tokens else 1
                print(f"📊 {duplicate_count}/{len(tokens)} duplicates ({ratio:.0%})")

                if ratio >= 0.3:
                    print(f"🛑 Page {page}: More than 30% duplicates — stopping.")

                    stop_condition = True

                if not stop_condition:
                    all_urls_to_push = new_tokens + duplicate_tokens
                else:
                    all_urls_to_push = new_tokens

                new_urls = [
                    {"content_url": f"https://api.divar.ir/v8/posts-v2/web/{t}"}
                    for t in all_urls_to_push
                ]
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

    print(f"✅ Extraction completed — {len(all_urls)} new urls extracted")
    return list(all_urls)
