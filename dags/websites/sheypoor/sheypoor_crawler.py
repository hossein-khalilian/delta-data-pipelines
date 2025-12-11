import asyncio
import json
import time
from urllib.parse import parse_qs, urlsplit

import httpx
import redis
from curl2json.parser import parse_curl

from utils.config import config


def extract_transform_urls(**kwargs):
    BLOOM_KEY = f"sheypoor_{config.get('redis_bloom_filter')}"

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

    # curl command
    try:
        with open(
            "./dags/websites/sheypoor/curl_commands/sheypoor_curl_command.txt",
            "r",
            encoding="utf-8",
        ) as file:
            curl_command = file.read()
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
    }

    # BASE URL
    BASE_URL = parsed_curl["url"].split("?")[0]

    # Initial parameters
    original_params = {}
    if "?" in parsed_curl["url"]:
        query = urlsplit(parsed_curl["url"]).query
        original_params = parse_qs(query)

        # Convert lists to single values
        for k, v in original_params.items():
            original_params[k] = v[0] if isinstance(v, list) and len(v) == 1 else v

    if "f" in original_params:
        original_params.pop("f")

    all_urls = []
    max_pages = 20
    stop_condition = False

    with httpx.Client(**client_params) as client:
        current_params = original_params.copy()

        for page in range(1, max_pages + 1):
            try:
                print(f" =========== Page: {page} =========== ")
                # Set current page
                current_params["p"] = str(page)

                # print(f"Requesting page {page} with parameters: {current_params}")
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
                    if rdb.execute_command("BF.EXISTS", BLOOM_KEY, url):
                        duplicate_count += 1
                        duplicate_ads_batch.append({"content_url": url})
                        continue

                    # Copy full ad + add content_url
                    full_ad = item.copy()
                    full_ad["content_url"] = url
                    new_ads_batch.append(full_ad)
                    # new_urls_batch.append({"content_url": url})

                total_found = len(items)
                ratio = duplicate_count / total_found if total_found > 0 else 1

                print(f"📊 Number of ads: {len(new_ads_batch)}")
                print(
                    f"📊 {duplicate_count}/{len(new_ads_batch)} duplicates ({ratio:.0%})"
                )

                if ratio >= 0.3:
                    print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                    stop_condition = True

                if not stop_condition:
                    ads_to_push = new_ads_batch + duplicate_ads_batch
                else:
                    ads_to_push = new_ads_batch

                all_urls.extend(ads_to_push)

                # update f for next page
                new_f = result.get("meta", {}).get("f")
                if new_f:
                    current_params["f"] = new_f
                    # print(f"f updated: {new_f}")

                if stop_condition:
                    break

                time.sleep(15)

            except Exception as e:
                print(f"Error on page {page}: {e}")
                break

    print(f"✅ Extraction completed — {len(all_urls)} new urls extracted")
    return all_urls
