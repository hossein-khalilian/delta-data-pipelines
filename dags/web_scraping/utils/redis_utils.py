import redis.asyncio as redis
from utils.config import config

async def add_to_bloom_filter(bloom_key, items):
    redis_url = config.get("redis_url")
    r = redis.from_url(redis_url, decode_responses=True)

    if not items:
        print(f"No items to add to {bloom_key}")
        return []

    result = await r.execute_command("BF.MADD", bloom_key, *items)
    # print(f"✅ BF.MADD result for {bloom_key}: {result}")
    await r.close()
    return result

async def check_bloom(bloom_key, items):
    if not items:
        return [], []

    redis_url = config.get("redis_url")
    r = redis.from_url(redis_url, decode_responses=True)

    # BF.MEXISTS  0,1
    exists_results = await r.execute_command("BF.MEXISTS", bloom_key, *items)

    new_items = [item for item, exists in zip(items, exists_results) if not exists]
    duplicate_items = [item for item, exists in zip(items, exists_results) if exists]

    await r.close()

    return new_items, duplicate_items

