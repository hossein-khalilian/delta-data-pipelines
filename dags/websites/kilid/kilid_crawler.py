import json
import time
import httpx
import redis
from curl2json.parser import parse_curl
from utils.config import config
from xml.etree import ElementTree as ET

def xml_to_json_bytesafe(xml_bytes):
    root = ET.fromstring(xml_bytes)

    items = []
    for elem in root.findall(".//result"):
        id_elem = elem.find("id")
        if id_elem is not None and id_elem.text:
            items.append({"id": id_elem.text})

    return {
        "data": {
            "result": {
                "result": items
            }
        }
    }

def extract_transform_urls():
    BLOOM_KEY = f"kilid_{config.get('redis_bloom_filter')}"
    print(f"✅ Using Bloom Filter: {BLOOM_KEY}")
    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"])

    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command(
                "BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000, "EXPANSION", 2
            )
            print(f"✅ Bloom filter named {BLOOM_KEY} has been created")
        except Exception as e:
            print(f"⚠️ Error while creating Bloom filter: {e}")
    # else:
    #     print(f"✅ Bloom filter named {BLOOM_KEY} already exists")

    # Load curl command
    try:
        with open("./dags/websites/kilid/curl_commands/kilid_curl_command.txt", "r", encoding="utf-8") as file:
            curl_command = file.read()
        print("✅ File kilid_curl_command.txt was read successfully")
    except Exception as e:
        print(f"❌ Error reading kilid_curl_command.txt: {e}")
        return

    parsed_curl = parse_curl(curl_command)
    parsed_curl.pop("cookies", None)

    # Load first request for cookies
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
        # print("=== Client Headers ===")
        # print(client.headers)

        # Get initial cookies
        resp = client.get("https://kilid.com")
        resp.raise_for_status()

        full_url = parsed_curl["url"]
        base_url = full_url.split("?")[0]
        query_string = full_url.split("?")[1] if "?" in full_url else ""
        base_params = dict(p.split("=") for p in query_string.split("&")) if query_string else {}
        base_params["sort"] = "searchDate_desc"

        for page in range(max_pages):
            try:
                base_params["page"] = str(page)

                response = client.request(
                    method=parsed_curl.get("method", "GET"),
                    url=base_url,
                    headers=client_params["headers"],
                    params=base_params,
                )
                response.raise_for_status()

                raw = response.content  

                try:
                    result = json.loads(raw)
                    # print(f"✅ Page {page}: JSON parsed")

                    if isinstance(result, list):
                        widgets = result
                    elif isinstance(result, dict):
                        widgets = result.get("data", {}).get("result", [])  
                    else:
                        widgets = []
                        print(f"⚠️ Page {page}: Unexpected JSON structure")


                except json.JSONDecodeError: 
                    print(f"⚠️ Page {page}: XML detected (bytesafe parsing)...")
                    try:
                        result = xml_to_json_bytesafe(raw)
                        count = len(result["data"]["result"]["result"])
                        print(f"✅ Page {page}: XML converted → JSON ({count} ids)")
                        widgets = result["data"]["result"]["result"]
                    except ET.ParseError as parse_err:
                        print(f"❌ XML Parse Error on page {page}: {parse_err}")
                        continue  

                ids = [w.get("id") for w in widgets if w.get("id")]

                if not ids:
                    print(f"⛔️ Page {page}: No IDs found, stopping.")
                    break

                print(f"Page: {page}")
                print(f"📊 Number of ads: {len(ids)}")
                # Bloom filter duplicate detection
                duplicate_count, new_ids, duplicate_ids = 0, [], []

                for id_val in ids:
                    url = f"https://kilid.com/detail/{id_val}"

                    exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, url)
                    if exists:
                        duplicate_count += 1
                        duplicate_ids.append(url)
                    else:
                        new_ids.append(url)
                        # rdb.execute_command("BF.ADD", BLOOM_KEY, url)

                ratio = duplicate_count / len(ids) if len(ids) > 0 else 0
                print(f"📊 {duplicate_count}/{len(ids)} duplicates ({ratio:.0%})")

                if ratio >= 0.3:
                    print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                    stop_condition = True

                all_ids_to_push = new_ids if stop_condition else new_ids + duplicate_ids

                new_urls = [{"content_url": url} for url in all_ids_to_push]
                all_urls.extend(new_urls)

                if stop_condition:
                    break

                time.sleep(1.5)

            except Exception as e:
                print(f"❌ Error on page {page}: {e}")
                break

    print(f"✅ Extraction completed — {len(all_urls)} URLs extracted")
    return list(all_urls)