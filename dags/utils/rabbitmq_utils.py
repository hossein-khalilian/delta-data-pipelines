import asyncio
import json
import logging
from datetime import datetime

import redis
from aio_pika import Message, connect_robust
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

from utils.config import config

logger = logging.getLogger(__name__)


def parse_message(body: bytes):
    decoded = body.decode("utf-8")
    # try:
    return json.loads(decoded)  # parse JSON
    # except json.JSONDecodeError:
    #     return decoded  # return as string if not JSON


class RabbitMQSensorTrigger(BaseTrigger):
    def __init__(self, queue_name: str, batch_size: int = 1, timeout: int = 60):
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.timeout = timeout

    def serialize(self):
        return (
            "utils.rabbitmq.rabbitmq_utils.RabbitMQSensorTrigger",
            {
                "queue_name": self.queue_name,
                "batch_size": self.batch_size,
                "timeout": self.timeout,
            },
        )

    async def run(self):
        messages = []
        start_time = datetime.now()

        try:
            connection = await connect_robust(
                host=config["rabbitmq_host"],
                port=config["rabbitmq_port"],
                login=config["rabbitmq_user"],
                password=config["rabbitmq_pass"],
            )
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(self.queue_name, passive=True)

                while True:
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                messages.append(parse_message(message.body))
                                logger.info(
                                    f"Trigger: Consumed {len(messages)} messages from {self.queue_name}"
                                )

                            if len(messages) >= self.batch_size:
                                yield TriggerEvent(
                                    {"status": "success", "messages": messages}
                                )
                                return

                    # Check timeout
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed >= self.timeout:
                        logger.warning(
                            f"Trigger: Timeout reached, returning {len(messages)} messages"
                        )
                        yield TriggerEvent({"status": "timeout", "messages": messages})
                        return

                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Trigger error: {e}")
            yield TriggerEvent(
                {"status": "error", "error": str(e), "messages": messages}
            )


class RabbitMQSensor(BaseSensorOperator):
    def __init__(
        self, queue_name: str, batch_size: int = 1, timeout: int = 60, **kwargs
    ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.timeout = timeout

    def execute(self, context):
        async def consume_batch():
            messages = []
            start_time = datetime.now()

            try:
                connection = await connect_robust(
                    host=config["rabbitmq_host"],
                    port=config["rabbitmq_port"],
                    login=config["rabbitmq_user"],
                    password=config["rabbitmq_pass"],
                )
                async with connection:
                    channel = await connection.channel()
                    queue = await channel.declare_queue(self.queue_name, passive=True)

                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                messages.append(parse_message(message.body))
                                # self.log.info(
                                #     f"Sensor: Consumed {len(messages)} messages"
                                # )

                            if len(messages) >= self.batch_size:
                                return messages

                            elapsed = (datetime.now() - start_time).total_seconds()
                            if elapsed >= self.timeout:
                                self.log.warning(
                                    f"Sensor: Timeout reached, returning {len(messages)} messages"
                                )
                                return messages
            except Exception as e:
                self.log.error(f"Sensor error: {e}")
                return messages

        messages = asyncio.run(consume_batch())

        self.log.info(f"✅ Consumed {len(messages)} URLs")

        if not messages:
            self.log.info(f"Deferring check for queue {self.queue_name}")
            raise self.defer(
                trigger=RabbitMQSensorTrigger(
                    queue_name=self.queue_name,
                    batch_size=self.batch_size,
                    timeout=self.timeout,
                ),
                timeout=self.timeout,
            )
        return messages


async def publish_messages(queue_name, messages):

    connection = await connect_robust(
        host=config["rabbitmq_host"],
        port=config["rabbitmq_port"],
        login=config["rabbitmq_user"],
        password=config["rabbitmq_pass"],
    )
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)

        # rdb = redis.Redis(
        #     host=config["redis_host"],
        #     port=config["redis_port"]
        # )

        for message in messages:
            token = message.get("content_url", "").split("/")[-1]

            # rdb.execute_command("BF.ADD", BLOOM_KEY, token)

            await channel.default_exchange.publish(
                Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=2,  # persistent
                ),
                routing_key=queue.name,
            )
    # print(f"✅ Sent {len(messages)} URLs to RabbitMQ")
