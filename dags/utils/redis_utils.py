import asyncio
import redis.asyncio as redis
from utils.config import config

async def add_to_bloom_filter(bloom_key, items):
    redis_host = config.get("redis_host")
    redis_port = config.get("redis_port")
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    result = await r.execute_command("BF.MADD", bloom_key, *items)
    print(f"✅ BF.MADD result for {bloom_key}: {result}")
    await r.close()
    return result

async def check_bloom(bloom_key, items):
    if not items:
        return [], []  

    redis_host = config.get("redis_host")
    redis_port = config.get("redis_port")
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # BF.MEXISTS  0,1
    exists_results = await r.execute_command("BF.MEXISTS", bloom_key, *items)
    
    new_items = [item for item, exists in zip(items, exists_results) if not exists]
    duplicate_items = [item for item, exists in zip(items, exists_results) if exists]
    
    await r.close()
    
       
    total = len(items)
    duplicate_count = len(duplicate_items)
    new_count = len(new_items)

    duplicate_percent = (duplicate_count / total) * 100 if total > 0 else 0

    print(f"new items: {new_items}")
    print(f"duplicate items: {duplicate_items}")
    print(f"New count: {new_count}")
    print(f"Duplicate count: {duplicate_count}")
    print(f"Duplicate percentage: {duplicate_percent:.2f}%")
    
    
    return new_items, duplicate_items