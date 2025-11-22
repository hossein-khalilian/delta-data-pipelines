import time
import httpx
import redis
from curl2json.parser import parse_curl
from utils.config import config


def extract_transform_urls():
    BLOOM_BUY = f"mrestate_buy_{config.get('redis_bloom_filter', 'v1')}"
    BLOOM_RENT = f"mrestate_rent_{config.get('redis_bloom_filter', 'v1')}"

    rdb = redis.Redis(host=config["redis_host"], port=config["redis_port"])

    for key in [BLOOM_BUY, BLOOM_RENT]:
        if not rdb.exists(key):
            try:
                rdb.execute_command("BF.RESERVE", key, 0.01, 1_000_000, "EXPANSION", 2)
                print(f"Bloom filter created: {key}")
            except:
                pass  

    modes = [
        ("buy",  "buy_mrestate_curl_command.txt",  BLOOM_BUY),
        ("rent", "rent_mrestate_curl_command.txt", BLOOM_RENT),
    ]

    all_urls = []
    total_new = 0  

    with httpx.Client(timeout=30.0) as client:
        for mode_name, curl_file, bloom_key in modes:
            print(f"\n{'='*60}")
            print(f"Starting MRESTATE.IR → {mode_name.upper()}")
            print(f"{'='*60}")

            # curl
            try:
                with open(f"./dags/websites/mrestate/curl_commands/{curl_file}", "r", encoding="utf-8") as f:
                    curl_cmd = f.read()
            except Exception as e:
                print(f"Cannot read {curl_file}: {e}")
                continue

            parsed = parse_curl(curl_cmd)
            base_url_template = parsed["url"]
            headers = parsed.get("headers", {})
            client.headers.update(headers)

            page = 1
            stop = False
            consecutive_empty = 0
            new_count = 0  
            dup_count = 0

            while page <= 500 and not stop:
                # pagination
                current_url = base_url_template.split("page=1")[0] + f"page={page}"

                try:
                    resp = client.get(current_url)
                    if resp.status_code != 200:
                        print(f"HTTP {resp.status_code} on page {page}")
                        break

                    data = resp.json()

                    # items
                    items = (
                        data.get("pageProps", {})
                        .get("dehydratedState", {})
                        .get("queries", [{}])[0]
                        .get("state", {})
                        .get("data", {})
                        .get("items", [])
                    )

                    if not items:
                        consecutive_empty += 1
                        print(f"Page {page} → empty ({consecutive_empty}/3)")
                        if consecutive_empty >= 3:
                            print(f"End of list or API changed → stopping at page {page}")
                            break
                        page += 1
                        time.sleep(1)
                        continue

                    consecutive_empty = 0
                    page_new = 0
                    page_dup = 0

                    for item in items:
                        slug = item.get("slug")
                        if not slug:
                            continue
                        exists = rdb.execute_command("BF.EXISTS", bloom_key, slug)
                        if exists:
                            page_dup += 1
                            dup_count += 1
                        else:
                            rdb.execute_command("BF.ADD", bloom_key, slug)
                            page_new += 1
                            new_count += 1
                            all_urls.append({"content_url": f"https://mrestate.ir/ad/{slug}"})

                    ratio = page_dup / len(items) if items else 1
                    print(f"Page {page:3d} → {len(items):2d} ads | New: {page_new:4d} | Dup: {page_dup:3d} ({ratio:.0%})")

                    if ratio > 0.5 and page > 15:
                        print(f"More than 50% duplicates → stopping at page {page}")
                        stop = True

                    page += 1
                    time.sleep(0.7)

                except Exception as e:
                    print(f"Error on page {page}: {e}")
                    time.sleep(5)
                    break  

            print(f"{mode_name.upper()} finished → {new_count} new ads collected")
            total_new += new_count

    print(f"\nTOTAL CRAWL COMPLETED: {total_new} new unique ads (buy + rent)")
    return all_urls