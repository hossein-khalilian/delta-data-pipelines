import asyncio
import os

from aio_pika import Message, connect_robust
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS")


async def main():
    # Connect to RabbitMQ
    connection = await connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASS,
    )

    async with connection:
        channel = await connection.channel()
        # Declare a durable queue
        queue = await channel.declare_queue("task_queue", durable=True)

        message_body = "Hello, RabbitMQ!"
        message = Message(
            body=message_body.encode(),
            delivery_mode=2,  # make message persistent
        )

        await channel.default_exchange.publish(message, routing_key=queue.name)

        print(f"Sent: {message_body}")


if __name__ == "__main__":
    asyncio.run(main())
