from pymongo import MongoClient
import pika
import json

MONGO_URI = "mongodb://appuser:appassword@172.16.36.111:27017/delta-datasets"
MONGO_DB = "delta-datasets"
MONGO_COLLECTION = "mrestate-dataset_1"

RABBITMQ_HOST = "172.16.36.111"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "admin"
RABBITMQ_PASS = "changethis"
RABBITMQ_URLS_QUEUE = "mrestate_urls_1"

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
