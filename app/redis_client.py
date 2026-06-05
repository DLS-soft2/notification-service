import redis.asyncio as aioredis

from app.config import settings

_redis: aioredis.Redis | None = None  # pylint: disable=invalid-name


def get_redis() -> aioredis.Redis:
    """Create (if needed), cache, and return the shared async Redis connection."""
    global _redis  # pylint: disable=global-statement
    if _redis is None:
        _redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def close_redis() -> None:
    """Close the shared async Redis connection."""
    global _redis  # pylint: disable=global-statement
    if _redis:
        await _redis.close()
        _redis = None
