import asyncio
import json
import logging
from datetime import datetime, timedelta
from aio_pika import Message, connect_robust
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from utils.config import config

logger = logging.getLogger(__name__)

def parse_message(body: bytes):
    decoded = body.decode("utf-8")

    return json.loads(decoded) # parse JSON

class RabbitMQSensorTrigger(BaseTrigger):
    def __init__(self, queue_name: str, batch_size: int = 1, timeout: int = 60):
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.timeout = timeout

    def serialize(self):
        return (
            "utils.rabbitmq_utils.RabbitMQSensorTrigger",
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

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            messages.append(parse_message(message.body))

                        if len(messages) >= self.batch_size:
                            yield TriggerEvent({"status": "success", "messages": messages})
                            return

                        # timeout check
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if elapsed >= self.timeout:
                            yield TriggerEvent({"status": "timeout", "messages": messages})
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
        self,
        queue_name: str,
        batch_size: int = 50,
        timeout: int = 60,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.timeout_seconds = timeout

    def execute(self, context):
        # ✅ always defer immediately
        self.log.info(f"Deferring sensor for queue: {self.queue_name}")

        self.defer(
            trigger=RabbitMQSensorTrigger(
                queue_name=self.queue_name,
                batch_size=self.batch_size,
                timeout=self.timeout_seconds,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout_seconds),
        )

    def execute_complete(self, context, event=None):
        if not event:
            raise ValueError("No event received from trigger")

        status = event.get("status")
        messages = event.get("messages", [])

        if status == "error":
            raise RuntimeError(event.get("error"))

        self.log.info(f"✅ Trigger returned {len(messages)} messages")

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

        for message in messages:
            token = message.get("content_url", "").split("/")[-1]

            await channel.default_exchange.publish(
                Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=2,  
                ),
                routing_key=queue.name,
            )
