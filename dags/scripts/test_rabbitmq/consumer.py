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

# Declare the same queue
channel.queue_declare(queue="task_queue", durable=True)


# Callback function to process messages
def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")
    # Acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Consume messages
channel.basic_qos(prefetch_count=1)  # Fair dispatch
channel.basic_consume(queue="task_queue", on_message_callback=callback)

print("Waiting for messages. To exit, press CTRL+C")
channel.start_consuming()
