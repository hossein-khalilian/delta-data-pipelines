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
            password=config["rabbitmq_password"],
            virtual_host=config["rabbitmq_vhost"],
        )
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(self.queue_name, durable=True)
            if queue.message_count > 0:
                yield self._done()
            else:
                await asyncio.sleep(5)


class RabbitMQSensor(BaseSensorOperator):
    def __init__(self, queue_name, **kwargs):
        super().__init__(**kwargs)
        self.queue_name = queue_name

    def execute(self, context):
        connection = asyncio.get_event_loop().run_until_complete(
            connect_robust(
                host=config["rabbitmq_host"],
                port=config["rabbitmq_port"],
                login=config["rabbitmq_user"],
                password=config["rabbitmq_password"],
                virtual_host=config["rabbitmq_vhost"],
            )
        )

        async def check():
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(self.queue_name, durable=True)
                return queue.message_count > 0

        has_message = asyncio.get_event_loop().run_until_complete(check())
        if not has_message and self.deferrable:
            raise self.defer(
                trigger=RabbitMQSensorTrigger(self.queue_name),
                timeout=self.timeout,
            )
        return has_message

