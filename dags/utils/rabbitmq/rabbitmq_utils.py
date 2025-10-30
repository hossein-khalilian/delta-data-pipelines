import asyncio

from aio_pika import connect_robust
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger

from utils.config import config


class RabbitMQSensorTrigger(BaseTrigger):
    def __init__(self, queue_name):
        self.queue_name = queue_name

    def serialize(self):
        return (
            "utils.rabbitmq_utils.RabbitMQSensorTrigger",
            {"queue_name": self.queue_name},
        )

    async def run(self):
        connection = await connect_robust(
            host=config["rabbitmq_host"],
            port=config["rabbitmq_port"],
            login=config["rabbitmq_user"],
            password=config["rabbitmq_pass"],
        )
        async with connection:
            channel = await connection.channel()
            try:
                queue_info = await channel.get_queue(self.queue_name)
                if queue_info.message_count > 0:
                    self.log.info(f"Trigger: Found {queue_info.message_count} messages in {self.queue_name}")
                    yield self._done()
                else:
                    await asyncio.sleep(5)
            except Exception as e:
                self.log.warning(f"Trigger error: {e}")
                await asyncio.sleep(5)

class RabbitMQSensor(BaseSensorOperator):
    def __init__(self, queue_name, **kwargs):
        super().__init__(**kwargs)
        self.queue_name = queue_name

    def execute(self, context):
        loop = asyncio.get_event_loop()

        async def check():
            connection = await connect_robust(
                host=config["rabbitmq_host"],
                port=config["rabbitmq_port"],
                login=config["rabbitmq_user"],
                password=config["rabbitmq_pass"],
                # virtual_host حذف شد
            )
            async with connection:
                channel = await connection.channel()
                try:
                    await channel.declare_queue(self.queue_name, durable=True, passive=True)
                    queue_info = await channel.get_queue(self.queue_name)
                    self.log.info(f"Sensor: {queue_info.message_count} messages in {self.queue_name}")
                    return queue_info.message_count > 0
                except Exception as e:
                    self.log.warning(f"Sensor error: {e}")
                    return False

        has_message = loop.run_until_complete(check())

        if not has_message and self.deferrable:
            self.log.info(f"Deferring check for queue {self.queue_name}")
            raise self.defer(
                trigger=RabbitMQSensorTrigger(self.queue_name),
                timeout=self.timeout,
            )
        return has_message