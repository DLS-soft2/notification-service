import redis.asyncio as aioredis

from app.config import settings

_redis: aioredis.Redis | None = None  # pylint: disable=invalid-name


async def start_redis() -> None:
    """Initialise the shared async Redis connection."""
    global _redis  # pylint: disable=global-statement
    _redis = aioredis.from_url(settings.redis_url, decode_responses=True)


async def stop_redis() -> None:
    """Close the shared async Redis connection."""
    if _redis:
        await _redis.aclose()


def get_redis() -> aioredis.Redis:
    """Return the initialised Redis client or raise if not started."""
    if _redis is None:
        raise RuntimeError("Redis not initialized — call start_redis() first")
    return _redis
