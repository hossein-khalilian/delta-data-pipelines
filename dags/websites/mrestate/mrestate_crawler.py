import time
import httpx
import redis
from curl2json.parser import parse_curl
from urllib.parse import urlparse, urlencode, parse_qs 
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
        ("buy", "mrestate_curl_command.txt", BLOOM_BUY, "buy_residential_apartment"),
        ("rent", "mrestate_curl_command.txt", BLOOM_RENT, "rent_residential_apartment"),
    ]

    all_urls = []
    total_new = 0

    with httpx.Client(timeout=30.0) as client:
        for mode_name, curl_file, bloom_key, mode_value in modes:
            print(f"Starting MRESTATE.IR → {mode_name.upper()}")

            # curl
            try:
                with open(f"./dags/websites/mrestate/curl_commands/{curl_file}", "r", encoding="utf-8") as f:
                    curl_template = f.read()
            except Exception as e:
                print(f"Cannot read {curl_file}: {e}")
                continue

            # جایگزینی {{mode}} در curl
            curl_cmd = curl_template.replace("{{mode}}", mode_value)
            
            parsed = parse_curl(curl_cmd)
            base_url_template = parsed["url"]
            headers = parsed.get("headers", {})
            client.headers.update(headers)
            
            client.headers.update({
                "accept-encoding": "gzip, deflate, br",
                "priority": "u=1, i",
                "sec-fetch-user": "?1"
            })
            page = 1
            stop = False
            # consecutive_empty = 0
            new_count = 0
            dup_count = 0

            while page <= 10 and not stop:
                # # pagination
                # # current_url = base_url_template.split("page=1")[0] + f"page={page}"
                # if page == 1:
                #     current_url = base_url
                # else:
                #     separator = "&" if "?" in base_url else "?"
                #     current_url = f"{base_url}{separator}page={page}"


                # ساخت URL با pagination استاندارد و تمیز
                parsed_url = urlparse(base_url_template)

                query_params = parse_qs(parsed_url.query)
                # query_params["page"] = [str(page)]  
                if page == 1:
                    query_params.pop("page", None)
                else:
                    query_params["page"] = [str(page)]

                new_query = urlencode(query_params, doseq=True)
                current_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"

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
                        print(f"Page {page} → empty, but continuing (no early stop on empty)")
                        page += 1
                        time.sleep(1.5)
                        continue

                    # consecutive_empty = 0
                    page_new = 0
                    page_dup = 0

                    for item in items:
                        u_id = item.get("u_id_file")
                        if not u_id:
                            continue
                        
                        # slug_key = u_id
                        
                        exists = rdb.execute_command("BF.EXISTS", bloom_key, u_id)
                        if exists:
                            page_dup += 1
                            dup_count += 1
                        else:
                            rdb.execute_command("BF.ADD", bloom_key, u_id)
                            page_new += 1
                            new_count += 1

                            api_url = f"https://mrestate.ir/_next/data/aRMoB8bhXCFS2CAkvXsyf/s/{u_id}.json?post={u_id}"
                            all_urls.append({"content_url": api_url})

                    ratio = page_dup / len(items) if items else 0
                    print(f"Page {page:3d} → {len(items):2d} ads | New: {page_new:4d} | Dup: {page_dup:3d} ({ratio:.1%})")

                    # دقیقاً همون چیزی که گفتی: هر وقت داپلیکیت >= 30% شد → تموم
                    if ratio >= 0.30:
                        print(f"STOPPING CRAWL: {ratio:.1%} duplicate ads on page {page}")
                        stop = True
                        break  # اختیاری: برای اطمینان بیشتر

                    page += 1
                    time.sleep(1.5)
                    
                except Exception as e:
                    print(f"Error on page {page}: {e}")
                    time.sleep(1.5)
                    break

            print(f"{mode_name.upper()} finished → {new_count} new ads collected")
            total_new += new_count

    print(f"\nTOTAL CRAWL COMPLETED: {total_new} new unique ads (buy + rent)")
    return all_urls