import redis.asyncio as aioredis

from app.config import settings

_redis_instance: aioredis.Redis | None = None  # pylint: disable=invalid-name


def get_redis() -> aioredis.Redis:
    """Create and return an async Redis client from the configured URL."""
    global _redis_instance  # pylint: disable=global-statement
    _redis_instance = aioredis.from_url(settings.redis_url)
    return _redis_instance


async def close_redis() -> None:
    """Close the active Redis connection."""
    global _redis_instance  # pylint: disable=global-statement
    if _redis_instance is not None:
        await _redis_instance.aclose()
        _redis_instance = None
