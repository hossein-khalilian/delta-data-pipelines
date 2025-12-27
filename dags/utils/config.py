import os

from dotenv import load_dotenv

load_dotenv()

ENV_VARS = [
    "USER_AGENT_DEFAULT",
    "REDIS_URL",
    "REDIS_BLOOM_FILTER",
    "MONGO_URI",
    "MONGO_DB",
    "MONGO_COLLECTION",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_USER",
    "RABBITMQ_PASS",
    "RABBITMQ_URLS_QUEUE",
    "SQL_HOST",
    "SQL_PORT",
    "SQL_NAME",
    "SQL_USER",
    "SQL_PASSWORD",
    "SEARCH_ENDPOINT_URL",
    "SEARCH_ENGINE_ACCESS_TOKEN",

]

config = {var.lower(): os.getenv(var) for var in ENV_VARS}

# int
config = config.copy()
config["rabbitmq_port"] = (
    int(config["rabbitmq_port"]) if config["rabbitmq_port"] else 5672
)
