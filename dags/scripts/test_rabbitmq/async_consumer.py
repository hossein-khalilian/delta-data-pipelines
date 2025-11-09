import asyncio
import os

from aio_pika import IncomingMessage, connect_robust
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
        # Fair dispatch
        await channel.set_qos(prefetch_count=1)

        # Declare queue
        queue = await channel.declare_queue("task_queue", durable=True)

        async def callback(message: IncomingMessage):
            async with message.process():
                print(f"Received: {message.body.decode()}")

        # Start consuming
        await queue.consume(callback)

        print("Waiting for messages. To exit, press CTRL+C")
        # Keep the program running
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    asyncio.run(main())
