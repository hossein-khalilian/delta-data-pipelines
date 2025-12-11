import json
import time
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
import redis
from bs4 import BeautifulSoup
from curl2json.parser import parse_curl

from utils.config import config


def extract_build_id(client):
    curl_file = "extract_buildid.txt"
    try:
        with open(
            f"./dags/websites/mrestate/curl_commands/{curl_file}", "r", encoding="utf-8"
        ) as f:
            curl_cmd = f.read()
    except Exception as e:
        print(f"Cannot read {curl_file}: {e}")
        return None

    parsed = parse_curl(curl_cmd)
    url = parsed["url"]
    headers = parsed.get("headers", {})
    client.headers.update(headers)

    try:
        resp = client.get(url)
        if resp.status_code != 200:
            print(f"Failed to fetch main page: HTTP {resp.status_code}")
            return None

        html = resp.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        next_data_script = soup.find("script", id="__NEXT_DATA__")

        if next_data_script:
            data = json.loads(next_data_script.string)
            build_id = data.get("buildId")
            if build_id:
                print(f"Extracted Build ID: {build_id}")
                return build_id
            else:
                print("Build ID not found in __NEXT_DATA__")
        else:
            print("__NEXT_DATA__ tag not found.")
    except Exception as e:
        print(f"Error extracting buildId: {e}")
    return None


def extract_transform_urls(website_conf=None):
    BLOOM_KEY = f"mrestate_{config.get('redis_bloom_filter')}"

    rdb = redis.from_url(config["redis_url"])

    if not rdb.exists(BLOOM_KEY):
        try:
            rdb.execute_command(
                "BF.RESERVE", BLOOM_KEY, 0.01, 1_000_000, "EXPANSION", 2
            )
            print(f"✅ Bloom filter '{BLOOM_KEY}' created")
        except Exception as e:
            print(f"⚠️ Error while creating Bloom filter: {e}")
    else:
        print(f"✅ Using Bloom Filter: {BLOOM_KEY}")

    modes = [
        ("buy", "mrestate_curl_command.txt", "buy_residential_apartment"),
        ("rent", "mrestate_curl_command.txt", "rent_residential_apartment"),
    ]

    all_urls = []
    total_new = 0

    with httpx.Client(timeout=30.0) as client:
        build_id = extract_build_id(client)

        for mode_name, curl_file, mode_value in modes:
            print(f"⚪Starting → {mode_name.upper()}")

            try:
                with open(
                    f"./dags/websites/mrestate/curl_commands/{curl_file}",
                    "r",
                    encoding="utf-8",
                ) as file:
                    curl_template = file.read()
            except Exception as e:
                print(f"❌ Error reading file curl_command_01.txt: {e}")
                return

            curl_cmd = curl_template.replace("{{mode}}", mode_value).replace(
                "{{build_id}}", build_id
            )

            parsed = parse_curl(curl_cmd)
            base_url_template = parsed["url"]
            headers = parsed.get("headers", {})
            client.headers.update(headers)

            client.headers.update(
                {
                    "accept-encoding": "gzip, deflate, br",
                    "priority": "u=1, i",
                    "sec-fetch-user": "?1",
                }
            )
            page = 1
            stop = False
            new_count = 0
            dup_count = 0

            while page <= 10 and not stop:

                parsed_url = urlparse(base_url_template)
                query_params = parse_qs(parsed_url.query)

                if page == 1:
                    query_params.pop("page", None)
                else:
                    query_params["page"] = [str(page)]

                new_query = urlencode(query_params, doseq=True)
                current_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"

                print(f" =========== Page: {page} =========== ")

                try:
                    resp = client.get(current_url)
                    if resp.status_code != 200:
                        print(f"HTTP {resp.status_code} on page {page} → stopping")
                        break

                    data = resp.json()

                    page_props = data.get("pageProps", {})
                    main_data = page_props.get("data", {})
                    inner_data = main_data.get("data", {})
                    items = inner_data.get("courses", [])

                    if not items:
                        print(f"Page {page} → empty, continue")
                        page += 1
                        time.sleep(1.5)
                        continue

                    page_new = 0
                    page_dup = 0

                    for item in items:
                        u_id = item.get("u_id_file")
                        if not u_id:
                            continue

                        api_url = f"https://mrestate.ir/_next/data/{build_id}/s/{u_id}.json?post={u_id}"
                        exists = rdb.execute_command("BF.EXISTS", BLOOM_KEY, api_url)

                        if exists:
                            page_dup += 1
                            dup_count += 1
                            continue

                        page_new += 1
                        new_count += 1

                        all_urls.append({"content_url": api_url})

                    ratio = page_dup / len(items) if items else 0
                    print(f"📊 Number of ads: {len(items)}")
                    print(f"📊 {page_dup}/{len(items)} duplicates ({ratio:.0%})")

                    if ratio >= 0.30:
                        print(f"🛑 Page {page}: More than 30% duplicates — stopping.")
                        stop = True
                        break
                    page += 1
                    time.sleep(1.5)

                except Exception as e:
                    print(f"Error on page {page}: {e}")
                    break

            print(f"{mode_name.upper()} finished → {new_count} new urls extracted")
            total_new += new_count

    print(f"✅ Extraction completed — {total_new} new urls extracted(buy + rent)")
    return all_urls

