import json
import time
import httpx
import redis
from curl2json.parser import parse_curl
from utils.config import config
import xml.etree.ElementTree as ET  

# ETL for crawler DAG
def extract_transform_urls():
    BLOOM_KEY = f"kilid_{config.get('redis_bloom_filter')}"

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

    # curl_command for search
    try:
        with open(
            "./dags/websites/kilid/curl_commands/kilid_curl_command.txt",
            "r",
            encoding="utf-8",
        ) as file:
            curl_command = file.read()
        print("✅ File kilid_curl_command.txt was read successfully")
    except Exception as e:
        print(f"❌ Error reading file kilid_curl_command.txt: {e}")
        return

    parsed_curl = parse_curl(curl_command)
    parsed_curl.pop("cookies", None)

    try:
        with open("./dags/websites/kilid/curl_commands/first_request.txt", "r", encoding="utf-8") as file:
            first_request_curl = file.read().replace("\\", "")
        print("✅ first_request.txt loaded successfully")
    except Exception as e:
        print(f"❌ Error reading first_request.txt: {e}")
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
        print("=== Client Headers ===")
        print(client.headers)

        # GET for cookies (already in cookies, but simulate if needed)
        # try:
        resp = client.get("https://kilid.com")
        resp.raise_for_status()
            # print("✅ Cookies received/updated")
        # except Exception as e:
        #     print(f"❌ Error fetching cookies: {e}")
        #     raise RuntimeError("Task failed because cookies could not be fetched")


        # Base url and params from parsed_curl
        base_url = parsed_curl["url"].split("?")[0]
        query_string = parsed_curl["url"].split("?")[1] if "?" in parsed_curl["url"] else ""
        base_params = dict(p.split("=") for p in query_string.split("&")) if query_string else {}
        base_params["sort"] = "searchDate_desc"

        for page in range(max_pages):
            try:
                # Update pagination
                base_params["page"] = str(page)
                parsed_curl["params"] = base_params
                
                response = client.request(
                    method=parsed_curl.get("method", "GET"),
                    url=parsed_curl["url"],
                    headers=parsed_curl.get("headers", {}),
                    params=parsed_curl.get("params"),
                )
                response.raise_for_status()
                
                response.encoding = 'utf-8'
                
                # Parse JSON (primary)
                try:
                    result = response.json()
                    print(f"✅ Page {page}: JSON ({len(result.get('data', {}).get('result', {}).get('result', []))} ads)")
                except json.JSONDecodeError:
                    (f"⚠️ Page {page}: XML detected, parsing...")
                    root = ET.fromstring(response.text)
                    ids = [res.find('id').text for res in root.findall(".//result") if res.find('id') is not None]
                    result = {'data': {'result': {'result': [{'id': iid} for iid in ids]}}}
                    print(f"✅ Page {page}: XML parsed ({len(ids)} ids)")    
                    
                # Extract ids
                widgets = result.get("data", {}).get("result", {}).get("result", []) or []
                ids = [w.get("id") for w in widgets if w.get("id")]
                if not ids:
                    print(f"⛔️ Page {page}: No IDs found, stopping.")
                    break

                print(f"Page: {page}")
                print(f"📊 Number of ads: {len(widgets)}")

                # Check for duplicates with Bloom
                duplicate_count, new_ids, duplicate_ids = 0, [], []
                for id_val in ids:
                    exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, id_val)
                    if exists:
                        duplicate_count += 1
                        duplicate_ids.append(id_val)
                    else:
                        new_ids.append(id_val)

                ratio = duplicate_count / len(ids) if ids else 1
                print(f"📊 {duplicate_count}/{len(ids)} duplicates ({ratio:.0%})")

                if ratio >= 0.3:
                    print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                    stop_condition = True

                if not stop_condition:
                    all_ids_to_push = new_ids + duplicate_ids
                else:
                    all_ids_to_push = new_ids

                new_urls = [
                    {"content_url": f"https://kilid.com/detail/{id_val}"}
                    for id_val in all_ids_to_push
                ]
                all_urls.extend(new_urls)

                if stop_condition:
                    break

                # Optional: Update pagination from response if available
                # Kilid doesn't have explicit next_page, so just increment

                time.sleep(1.5)

            except Exception as e:
                print(f"❌ Error requesting page {page}: {e}")
                break

    print(f"✅ Extraction completed — {len(all_urls)} new urls extracted")
    
    return list(all_urls)