import os
import json
import redis
import pika
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# Mongo 
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "divar-" + os.getenv("MONGO_COLLECTION")

# Redis Bloom
REDIS_URL = os.getenv("REDIS_URL")
REDIS_BLOOM_FILTER = "divar_" + os.getenv("REDIS_BLOOM_FILTER")

# RabbitMQ
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_URLS_QUEUE")

RABBIT_URL = (
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}"
    f"@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
)
# Mongo
def check_url_in_mongo(url: str):
    try:
        client = MongoClient(MONGO_URI)
        collection = client[MONGO_DB][MONGO_COLLECTION]
        return collection.find_one({"content_url": url}) is not None
    except Exception as e:
        print("⚠️ MongoDB error:", e)
        return None


# Bloom Filter
def check_url_in_bloom(url: str):
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        return bool(r.execute_command("BF.EXISTS", REDIS_BLOOM_FILTER, url))
    except Exception as e:
        print("⚠️ Bloom error:", e)
        return None


# RabbitMQ
def check_url_in_rabbitmq(url: str):
    try:
        params = pika.URLParameters(RABBIT_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        found = False

        while True:
            method, properties, body = channel.basic_get(
                queue=RABBITMQ_QUEUE,
                auto_ack=False
            )

            if method is None:
                break

            try:
                message = json.loads(body)
            except json.JSONDecodeError:
                message = {}

            if message.get("content_url") == url:
                found = True

            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=True
            )

            if found:
                break

        connection.close()
        return found

    except Exception as e:
        print("⚠️ RabbitMQ error:", e)
        return None


if __name__ == "__main__":
    token = input("enter the token: ").strip()
    url = f"https://api.divar.ir/v8/posts-v2/web/{token}"

    print("\n🔍 Checking URL:")
    print(url)
    print("-" * 40)

    mongo_result = check_url_in_mongo(url)
    bloom_result = check_url_in_bloom(url)
    rabbit_result = check_url_in_rabbitmq(url)

    print("MongoDB:      ", "✅ EXISTS" if mongo_result else "❌ NOT FOUND")
    print("Bloom Filter: ", "✅ EXISTS" if bloom_result else "❌ NOT FOUND")
    print("RabbitMQ:     ", "✅ EXISTS" if rabbit_result else "❌ NOT FOUND")
