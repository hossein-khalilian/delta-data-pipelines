# /home/Kowsar/DELTA/token/dag_divar_crawler.py
# DAG Airflow برای استخراج توکن‌های دیوار، ذخیره در فیلتر بلوم Redis، صف‌بندی در Redis،
# مصرف از صف Redis، دریافت محتوا، تبدیل، و ذخیره در MongoDB.

from datetime import datetime, timedelta
import json
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.operators.python import PythonOperator

import redis
from pymongo import MongoClient

# تنظیمات crawler
USER_AGENT_DEFAULT = "DivarTokenCrawler/1.0 (+https://example.com) (respect-robots.txt)"
TOKEN_REGEX = re.compile(r"/v/[^/]+/([A-Za-z0-9_-]+)(?:$|/)", flags=0)
FOLLOW_PATH_KEYWORDS = ("/s/tehran/buy-apartment", "page=")

# تنظیمات Redis
REDIS_HOST = '172.16.36.111'
REDIS_PORT = 6379
REDIS_BLOOM_FILTER = 'divar_tokens_bloom'
REDIS_QUEUE = 'divar_token_queue'  # لیست Redis برای صف‌بندی توکن‌ها

# تنظیمات MongoDB
MONGO_URI = 'mongodb://appuser:apppassword@172.16.36.111:27017/delta-datasets'
MONGO_DB = 'delta-datasets'
MONGO_COLLECTION = 'testkowsar3'

# نقطه پایانی API برای دریافت محتوای آگهی
DIVAR_API_URL = 'https://api.divar.ir/v8/posts-v2/web/{}'

def make_session(user_agent: str):
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept-Language": "fa-IR,en;q=0.9"})
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=(500,502,503,504))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

_robot_cache = {}

def can_fetch(url: str, user_agent: str) -> bool:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base in _robot_cache:
        rp = _robot_cache[base]
    else:
        robots_url = urljoin(base, "/robots.txt")
        rp = RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception:
            _robot_cache[base] = rp
            return False
        _robot_cache[base] = rp
    try:
        return rp.can_fetch(user_agent, url)
    except Exception:
        return False

def extract_tokens_from_href(base_url: str, href: str):
    if not href:
        return None
    full = urljoin(base_url, href)
    m = TOKEN_REGEX.search(urlparse(full).path)
    return m.group(1) if m else None

def extract_tokens_from_html(base_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    tokens = set()
    for a in soup.find_all("a", href=True):
        t = extract_tokens_from_href(base_url, a["href"])
        if t:
            tokens.add(t)
    for m in re.finditer(r"https?://[^\"\'>\s]+/v/[^/]+/([A-Za-z0-9_-]+)", html):
        tokens.add(m.group(1))
    return tokens

def should_follow_link(base_netloc: str, link: str):
    if not link:
        return False
    p = urlparse(link)
    if p.netloc and p.netloc != base_netloc:
        return False
    path_q = p.path + ("?" + p.query if p.query else "")
    lower = path_q.lower()
    return any(k in lower for k in FOLLOW_PATH_KEYWORDS)

def crawl_tokens(seed_urls, delay=1.5, max_pages=200, max_tokens=100, user_agent=USER_AGENT_DEFAULT):
    sess = make_session(user_agent)
    queue = deque(seed_urls)
    visited = set()
    found_tokens = set()
    pages_crawled = 0

    while queue and (max_pages <= 0 or pages_crawled < max_pages) and (len(found_tokens) < max_tokens):
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not can_fetch(url, user_agent):
            continue

        try:
            r = sess.get(url, timeout=15)
            r.raise_for_status()
        except Exception:
            time.sleep(delay)
            pages_crawled += 1
            continue

        html = r.text
        tokens = extract_tokens_from_html(url, html)
        if tokens:
            found_tokens.update(tokens)
            if len(found_tokens) >= max_tokens:
                break

        soup = BeautifulSoup(html, "html.parser")
        base_netloc = urlparse(url).netloc
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(url, href)
            if full in visited:
                continue
            if should_follow_link(base_netloc, full):
                queue.append(full)

        pages_crawled += 1
        time.sleep(delay)

    return list(found_tokens)[:max_tokens]

def extract_and_produce(**kwargs):
    seed_urls = ["https://divar.ir/s/tehran/buy-apartment"]
    tokens = crawl_tokens(seed_urls, max_tokens=100)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    new_tokens = []
    for token in tokens:
        if not r.execute_command('BF.EXISTS', REDIS_BLOOM_FILTER, token):
            r.execute_command('BF.ADD', REDIS_BLOOM_FILTER, token)
            r.lpush(REDIS_QUEUE, token)  # افزودن توکن به صف Redis
            new_tokens.append(token)
    
    print(f"تعداد {len(new_tokens)} توکن جدید به صف Redis اضافه شد.")

def consume_fetch_transform_store(**kwargs):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    token = r.rpop(REDIS_QUEUE)  # برداشتن توکن از صف Redis
    if token is None:
        print("هیچ توکنی در صف وجود ندارد.")
        return

    token = token.decode('utf-8')  # تبدیل از بایت به رشته
    url = DIVAR_API_URL.format(token)
    
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT_DEFAULT})
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"خطا در دریافت {token}: {e}")
        return

    # تبدیل‌ها
    transformed = {
        'token': token,
        'title': data.get('sections', [{}])[0].get('widgets', [{}])[0].get('data', {}).get('title'),
        'description': data.get('sections', [{}])[1].get('widgets', [{}])[0].get('data', {}).get('text'),
        'price': data.get('webengage', {}).get('price'),
        'location': data.get('webengage', {}).get('district'),
        'raw_data': data,
        'crawl_timestamp': datetime.utcnow()  # افزودن زمان‌بندی
    }

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.insert_one(transformed)
    print(f"داده‌های تبدیل‌شده برای {token} در MongoDB ذخیره شد.")

# DAG تولیدکننده: هر 5 دقیقه حدود 100 توکن استخراج می‌کند
producer_dag = DAG(
    'divar_token_producer',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 9, 28),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='استخراج توکن‌های دیوار و افزودن به صف Redis',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

# DAG مصرف‌کننده: هر 5 دقیقه یک توکن را پردازش می‌کند
consumer_dag = DAG(
    'divar_token_consumer',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 9, 28),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='مصرف از صف Redis، دریافت، تبدیل، و ذخیره در MongoDB',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_and_produce',
    python_callable=extract_and_produce,
    provide_context=True,
    dag=producer_dag,
)

consume_task = PythonOperator(
    task_id='consume_fetch_transform_store',
    python_callable=consume_fetch_transform_store,
    provide_context=True,
    dag=consumer_dag,
)