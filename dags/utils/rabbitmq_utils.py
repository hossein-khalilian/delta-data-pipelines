import asyncio
import json
import logging
from datetime import datetime, timedelta
from aio_pika import Message, connect_robust
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from utils.config import config
from aio_pika.exceptions import QueueEmpty

logger = logging.getLogger(__name__)

def parse_message(body: bytes):
    decoded = body.decode("utf-8")
    return json.loads(decoded)  # parse JSON

class RabbitMQSensorTrigger(BaseTrigger):
    def __init__(self, queue_name: str, batch_size: int = 1, timeout: int = 300):
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
                queue = await channel.declare_queue(self.queue_name, durable=True)

                while (datetime.now() - start_time).total_seconds() < self.timeout:
                    try:
                        message = await asyncio.wait_for(
                            queue.get(no_ack=False),
                            timeout=5
                        )
                    except (asyncio.TimeoutError, QueueEmpty):
                        continue

                    if message:
                        async with message.process():
                            messages.append(parse_message(message.body))
                        
                        if len(messages) >= self.batch_size:
                            yield TriggerEvent({"status": "success", "messages": messages})
                            return
                    
                # Timeout reached
                if messages:
                    logger.warning(f"Trigger: Timeout reached, returning {len(messages)} messages")
                    yield TriggerEvent({"status": "timeout", "messages": messages})
                else:
                    logger.info("Trigger: Timeout reached, queue was empty")
                    yield TriggerEvent({"status": "timeout", "messages": []})
        
        except Exception as e:
            logger.error("Trigger failed to connect or declare queue", exc_info=True)
            error_details = f"Real Connection Error: {repr(e)}"
            yield TriggerEvent({
                "status": "error",
                "error": error_details,
                "messages": messages or []
            })
        
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
            error_msg = event.get("error") or "Unknown trigger error"
            raise RuntimeError(error_msg)
        
        if status == "success":
            self.log.info(f"✅ Batch complete: Got {len(messages)} messages")
            return messages
        
        if status == "timeout":
            if messages:
                self.log.info(f"⏳ Timeout but got {len(messages)} messages")
                return messages
            else:
                self.log.info(f"⏳ Queue is empty, returning empty list")
                return []

        return messages

# Utility function to publish messages
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
            await channel.default_exchange.publish(
                Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=2,  # persistent
                ),
                routing_key=queue.name,
            )
