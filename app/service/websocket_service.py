import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def subscribe_to_notifications(customer_id: str) -> AsyncIterator[aioredis.client.PubSub]:
    """Subscribe to Redis pub/sub for a customer. Yields the PubSub to iterate messages.

    Creates a dedicated Redis connection (not the shared pool), subscribes to
    ``notifications:{customer_id}``, and guarantees cleanup on exit.
    """
    channel = f"notifications:{customer_id}"
    dedicated_redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    pubsub = dedicated_redis.pubsub()
    try:
        await pubsub.subscribe(channel)
        logger.info("Subscribed to %s", channel)
        yield pubsub
    finally:
        await pubsub.unsubscribe()
        await pubsub.aclose()
        await dedicated_redis.aclose()
