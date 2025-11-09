import os

import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS")

connection_params = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
)


connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Declare a queue (it must exist on both producer & consumer)
channel.queue_declare(queue="task_queue", durable=True)

# Send a message
message = "Hello, RabbitMQ!"
channel.basic_publish(
    exchange="",
    routing_key="task_queue",
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ),
)

print(f"Sent: {message}")
connection.close()
