import asyncio
import json
import time
import httpx
import redis
from urllib.parse import parse_qs, urlsplit
from curl2json.parser import parse_curl
from utils.config import config

def extract_transform_urls(**kwargs):
    website_conf = kwargs["website_conf"]                   # from DAG Factory 
    BLOOM_KEY = website_conf["redis_bloom_filter"]            # from YAML 
    
    print(f"Using Bloom Filter: {BLOOM_KEY}")
    print(config["redis_host"])
    print(config["redis_port"])
    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"])

    # Bloom filter
    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command("BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000, "EXPANSION", 2)
            print(f"Bloom filter named {BLOOM_KEY} has been created")
        except Exception as e:
            print(f"Error creating Bloom filter: {e}")
    else:
        print(f"Bloom filter {BLOOM_KEY} already exists")

    # curl command
    try:
        with open("./dags/sheypoor/utils/curl_commands/sheypoor_curl_command.txt", "r", encoding="utf-8") as file:
            curl_command = file.read()
        print("✅ File sheypoor_curl_command.txt was read successfully")
    except Exception as e:
        print(f"❌ Error reading file sheypoor_curl_command: {e}")
        return

    # Convert to dictionary
    parsed_curl = parse_curl(curl_command)
    parsed_curl.pop("cookies", None)

    # Client
    client_params = {
        "verify": True,
        "headers": parsed_curl.get("headers", {}),
        # "timeout": 20,
    }

    # BASE URL
    BASE_URL = parsed_curl["url"].split("?")[0]

    # پارامترهای اولیه (مثل f)
    original_params = {}
    if "?" in parsed_curl["url"]:
        query = urlsplit(parsed_curl["url"]).query
        original_params = parse_qs(query)
        # تبدیل لیست به مقدار تک (مثل f)
        for k, v in original_params.items():
            original_params[k] = v[0] if isinstance(v, list) and len(v) == 1 else v

    if "f" in original_params: 
        original_params.pop("f")

    all_urls = []
    max_pages = 10
    stop_condition = False

    with httpx.Client(**client_params) as client:
        current_params = original_params.copy()

        for page in range(1, max_pages+1):
            try:
                # Set current page
                current_params["p"] = str(page)

                print(f"Requesting page {page} with parameters: {current_params}")

                response = client.get(BASE_URL, params=current_params)
                response.raise_for_status()
                result = response.json()

                items = result.get("data", []) or []
                if not items:
                    print(f"⛔️ Page {page}: No tokens found, stopping.")
                    break

                # Extract and process complete ads
                duplicate_count = 0
                new_ads_batch = []
                duplicate_ads_batch = []

                for item in items:
                    
                    if item.get("type") != "normal":
                        continue
                    
                    item_id = item.get("id")
                    attr = item.get("attributes", {})
                    url = attr.get("url")

                    if not item_id or not url:
                        continue

                    # Check for duplicates
                    if rdb.execute_command("BF.EXISTS", BLOOM_KEY, item_id):
                        duplicate_count += 1
                        duplicate_ads_batch.append({"content_url": url})
                        continue

                    # Add to Bloom filter
                    rdb.execute_command("BF.ADD", BLOOM_KEY, item_id)

                    # Copy full ad + add content_url
                    full_ad = item.copy()
                    full_ad["content_url"] = url
                    new_ads_batch.append(full_ad)


                total_found = len(items)
                ratio = duplicate_count / total_found if total_found > 0 else 1
                print(f"Page {page}: {total_found} ads → {len(new_ads_batch)} new, {duplicate_count} duplicates ({ratio:.0%})")

                if ratio >= 0.3:
                    print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                    stop_condition = True

                if not stop_condition:
                    ads_to_push = new_ads_batch + duplicate_ads_batch
                else:
                    ads_to_push = new_ads_batch
                    
                all_urls.extend(ads_to_push)
                print(f"Page {page}: {len(ads_to_push)} ads added to output")

                # update f for next page
                new_f = result.get("meta", {}).get("f")
                if new_f:
                    current_params["f"] = new_f
                    print(f"f updated: {new_f}")

                if stop_condition:
                    break

                time.sleep(1.5)

            except Exception as e:
                print(f"Error on page {page}: {e}")
                break

    kwargs["ti"].xcom_push(key="extracted_urls", value=all_urls)
    print(f"استخراج کامل شد — {len(all_urls)} آگهی جدید به XCom ارسال شد.")


def produce_to_rabbitmq(**kwargs):
    urls = kwargs["ti"].xcom_pull(key="extracted_urls", task_ids="extract_transform_task")
    if not urls:
        print("هیچ URL برای ارسال وجود ندارد.")
        return
    # asyncio.run(publish_tokens(urls))