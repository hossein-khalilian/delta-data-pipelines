import os

from dotenv import find_dotenv, load_dotenv

load_dotenv()

ENV_VARS = [
    "USER_AGENT_DEFAULT",
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_BLOOM_FILTER",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "MONGO_URI",
    "MONGO_DB",
    "MONGO_COLLECTION",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_USER",
    "RABBITMQ_PASS",
]

config = {var.lower(): os.getenv(var) for var in ENV_VARS}
