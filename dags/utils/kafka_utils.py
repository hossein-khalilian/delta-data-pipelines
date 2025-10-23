from kafka import KafkaConsumer, KafkaProducer
from airflow.utils.decorators import apply_defaults
from airflow.sensors.base import BaseSensorOperator

from utils.config import config

class KafkaMessageSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = config["kafka_bootstrap_servers"]
        self.topic = config["kafka_topic"]

    def poke(self, context):
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                # auto_offset_reset="latest",
                auto_offset_reset="earliest",
                group_id="divar_sensor_group",
                enable_auto_commit=False,
            )
            messages = consumer.poll(timeout_ms=5000)
            has_messages = any(len(records) > 0 for records in messages.values())
            consumer.close()

            if has_messages:
                print(f"✅ A new message was found in topic '{self.topic}'.")
            else:
                print(
                    f"⚠️ No messages found in topic '{self.topic}'. Waiting for a new message..."
                )

            return has_messages

        except Exception as e:
            print(f"❌ Error while checking Kafka messages: {e}")
            return False