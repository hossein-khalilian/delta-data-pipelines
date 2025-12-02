from pymongo import MongoClient
import pika
import json
import os 

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "mrestate-dataset"

RABBITMQ_HOST = os.getenv("rabbitmq_host")
RABBITMQ_PORT = os.getenv("rabbitmq_port")
RABBITMQ_USER = os.getenv("rabbitmq_user")
RABBITMQ_PASS = os.getenv("rabbitmq_pass")
RABBITMQ_URLS_QUEUE = "mrestate_urls"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
)
channel = connection.channel()

channel.queue_declare(queue=RABBITMQ_URLS_QUEUE, durable=True)

cursor = collection.find({"title": None}, {"content_url": 1})

count = 0
for doc in cursor:
    content_url = doc.get("content_url")
    if not content_url:
        continue 

    message = json.dumps({"url": content_url})
    channel.basic_publish(
        exchange="",
        routing_key=RABBITMQ_URLS_QUEUE,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2) 
    )
    count += 1

print(f"\nDone. Total sent: {count}")

connection.close()
mongo_client.close()
