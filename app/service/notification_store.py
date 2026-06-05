import json

import redis.asyncio as aioredis


async def get_recent_notifications(redis_client: aioredis.Redis, customer_id: str, limit: int = 50) -> list[dict]:
    """Read the most recent notifications from the Redis history list.

    Returns up to `limit` notifications, most-recent first (natural LPUSH order).
    Returns an empty list when the key does not exist.
    """
    history_key = f"notifications:history:{customer_id}"
    raw_entries = await redis_client.lrange(history_key, 0, limit - 1)
    return [json.loads(entry) for entry in raw_entries]
