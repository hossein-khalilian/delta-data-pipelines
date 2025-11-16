import os

from dotenv import load_dotenv

load_dotenv()

ENV_VARS = [
    "USER_AGENT_DEFAULT",
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_BLOOM_FILTER",
    "MONGO_URI",
    "MONGO_DB",
    "MONGO_COLLECTION",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_USER",
    "RABBITMQ_PASS",
    "RABBITMQ_URLS_QUEUE",
]

config = {var.lower(): os.getenv(var) for var in ENV_VARS}

# int
config = config.copy()
config["redis_port"] = int(config["redis_port"]) if config["redis_port"] else 6379
config["rabbitmq_port"] = (
    int(config["rabbitmq_port"]) if config["rabbitmq_port"] else 5672
)
