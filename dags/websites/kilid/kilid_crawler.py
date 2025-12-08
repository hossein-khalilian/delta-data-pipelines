import json
import time
import httpx
import redis
from curl2json.parser import parse_curl
from utils.config import config
from xml.etree import ElementTree as ET
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def xml_to_json_bytesafe(xml_bytes):
    root = ET.fromstring(xml_bytes)

    items = []
    for elem in root.findall(".//result"):
        id_elem = elem.find("id")
        listing_type_elem = elem.find("listingType")
        property_type_elem = elem.find("propertyType")
        landuse_type_elem = elem.find("landuseType")
        
        if id_elem is not None and id_elem.text:
            item = {"id": id_elem.text}
            if listing_type_elem is not None:
                item["listingType"] = listing_type_elem.text
            if property_type_elem is not None:
                item["propertyType"] = property_type_elem.text
            if landuse_type_elem is not None:
                item["landuseType"] = landuse_type_elem.text
            items.append(item)

    return {
        "data": {
            "result": {
                "result": items
            }
        }
    }

def extract_transform_urls():
    BLOOM_KEY = f"kilid_{config.get('redis_bloom_filter')}"
    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"])

    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command("BF.RESERVE", BLOOM_KEY, 0.05, 1_000_000, "EXPANSION", 2)
            print(f"✅ Bloom filter '{BLOOM_KEY}' created")
        except Exception as e:
            print(f"⚠️ Error while creating Bloom filter: {e}")
    else:
        print(f"✅ Using Bloom Filter: {BLOOM_KEY}")

    # curl command
    try:
        with open("./dags/websites/kilid/curl_commands/kilid_curl_command.txt", "r", encoding="utf-8") as file:
            curl_command_template = file.read()
    except Exception as e:
        print(f"❌ Error reading kilid_curl_command.txt: {e}")
        return

    parsed_curl_template = parse_curl(curl_command_template)
    parsed_curl_template.pop("cookies", None)
    
    # CITY + TYPE 
    CITY_MAP = {
        "tehran": "272905",
        "karaj": "273014",
        "isfahan": "272903",
        "shiraz": "272896",
        "mashhad": "272895",
        "rasht": "242556",
        "sari": "242580",
        "tabriz": "242459",
        "qom": "242483",
    }

    TARGET_CITIES = list(CITY_MAP.keys())

    # Load first request for cookies
    try:
        with open("./dags/websites/kilid/curl_commands/first_request.txt", "r", encoding="utf-8") as file:
            first_request_curl = file.read().replace("\\", "")
    except Exception as e:
        print(f"❌ Error reading first_request.txt: {e}")
        return

    parsed_first = parse_curl(first_request_curl)
    cookies = parsed_first.pop("cookies", {})

    # client params
    client_params = {
        "verify": True,
        "cookies": cookies,
    }

    all_urls = []
    max_pages = 10

    with httpx.Client(**client_params) as client:

        total_extracted = 0

        modes = [
            ("BUY", TARGET_CITIES),
            ("RENT", TARGET_CITIES),
        ]

        for listing_type, city_list in modes:
            print(f"⚪ STARTING TYPE → {listing_type}")
            
            for city_key in city_list:

                # Parse curl once, without template replace
                parsed_curl = parse_curl(curl_command_template)

                mode_headers = parsed_curl.get("headers", {})
                if isinstance(mode_headers, dict):
                    client.headers.update(mode_headers)

                base_url = parsed_curl.get("url", "")
                if not base_url:
                    print(f"❌ No URL found in curl template")
                    continue

                parsed_url = urlparse(base_url)
                original_params = parse_qs(parsed_url.query)

                # Dynamically override city & type
                original_params["city"] = [CITY_MAP[city_key]]
                original_params["listingType"] = [listing_type]

                page = 0
                stop = False
                new_count = 0
                dup_count = 0

                while page <= max_pages and not stop:
                    query_params = original_params.copy()

                    query_params["page"] = [str(page)]
                    query_params["sort"] = ["searchDate_desc"]

                    new_query = urlencode(query_params, doseq=True)

                    current_url = urlunparse((
                        parsed_url.scheme,
                        parsed_url.netloc,
                        parsed_url.path,
                        "",
                        new_query,
                        ""
                    ))

                    print(f" ========== Page: {page} | {city_key} ========== ")

                    try:
                        response = client.request(
                            method=parsed_curl.get("method", "GET"),
                            url=current_url,
                            headers=mode_headers,
                            timeout=30.0,
                        )
                        response.raise_for_status()
                        
                        raw = response.content
                        
                        # -------------------------
                        # if not raw:
                        #     print(f"⚠️ Page {page}: empty")
                        #     break
                        # print(f"📄 RAW RESPONSE SAMPLE (Page {page}): {response.text[:300]}...")
                        # -------------------------
                        
                        try:
                            result = json.loads(response.text)
                            if isinstance(result, list):
                                widgets = result
                            elif isinstance(result, dict):
                                widgets = result.get("data", {}).get("result", [])
                                if not isinstance(widgets, list):
                                    print(f"⚠️ Unexpected JSON structure: widgets is {type(widgets)}")
                                    widgets = []
                            else:
                                widgets = []
                                print(f"⚠️ Page {page}: Unexpected JSON structure")
                        except json.JSONDecodeError:
                            print(f"⚠️ Page {page}: XML detected (bytesafe parsing)...")
                            try:
                                xml_converted = xml_to_json_bytesafe(raw)
                                widgets = xml_converted["data"]["result"]["result"]
                                count = len(widgets)
                                print(f"✅ Page {page}: XML converted → JSON ({count} ids)")
                            except ET.ParseError as parse_err:
                                print(f"❌ XML Parse Error on page {page}: {parse_err}")
                                widgets = []

                        if not widgets:
                            print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                            break

                        page_new = 0
                        page_dup = 0

                        for w in widgets:
                            id_val = w.get("id")
                            if not id_val:
                                continue
                            detail_url = f"https://kilid.com/detail/{id_val}"

                            exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, detail_url)
                            if exists:
                                page_dup += 1
                                dup_count += 1
                                continue

                            page_new += 1
                            new_count += 1
                            all_urls.append({
                                "content_url": detail_url,
                                "listingType": w.get("listingType"),
                                "propertyType": w.get("propertyType"),
                                "landuseType": w.get("landuseType")
                            })

                        ratio = page_dup / len(widgets) if len(widgets) > 0 else 0
                        print(f"📊 Number of ads: {len(widgets)}")
                        print(f"📊 {page_dup}/{len(widgets)} duplicates ({ratio:.0%})")

                        if ratio >= 0.30:
                            print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                            stop = True
                            break

                        page += 1
                        time.sleep(5)

                    except Exception as e:
                        print(f"❌ Error on page {page} for {city_key}/{listing_type}: {e}")
                        break

                print(f"{city_key} / {listing_type} finished → {new_count} new urls extracted")

                total_extracted += new_count
                time.sleep(5)
                
            time.sleep(5)


        print(f"✅ Extraction completed — {total_extracted} new urls extracted (all modes)")
        return all_urls