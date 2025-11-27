import asyncio

import redis.asyncio as redis
from utils.config import config


async def add_to_bloom_filter(bloom_key, items):
    redis_host = config.get("redis_host")
    redis_port = config.get("redis_port")
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    result = await r.execute_command("BF.MADD", bloom_key, *items)
    await r.close()
    return result
