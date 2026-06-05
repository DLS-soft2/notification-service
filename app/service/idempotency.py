import redis.asyncio as aioredis

from app.config import settings


async def is_duplicate(redis_client: aioredis.Redis, event_id: str) -> bool:
    """Atomically check and mark an event as processed. Returns True if already processed."""
    key = f"idempotency:{event_id}"
    result = await redis_client.set(key, "1", nx=True, ex=settings.idempotency_ttl_seconds)
    return result is None
