import json
from datetime import datetime, timezone

import redis.asyncio as aioredis

RECEIPT_TTL_SECONDS = 604800  # 7 days


async def generate_delivery_receipt(
    event_data: dict, redis_client: aioredis.Redis,
) -> dict | None:
    """Build a delivery receipt from event data and store it in Redis.

    Returns the receipt dict, or None if required fields are missing.
    """
    order_id = event_data.get("order_id")
    customer_id = event_data.get("customer_id")
    timestamp = event_data.get("timestamp")

    if not order_id or not customer_id or not timestamp:
        return None

    receipt = {
        "order_id": str(order_id),
        "customer_id": str(customer_id),
        "delivered_at": str(timestamp),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "delivered",
    }

    key = f"delivery_receipt:{order_id}"
    await redis_client.set(key, json.dumps(receipt), ex=RECEIPT_TTL_SECONDS)
    return receipt


async def get_delivery_receipt(
    order_id: str, redis_client: aioredis.Redis,
) -> dict | None:
    """Read a delivery receipt from Redis. Returns None if not found."""
    raw = await redis_client.get(f"delivery_receipt:{order_id}")
    if raw is None:
        return None
    return json.loads(raw)
