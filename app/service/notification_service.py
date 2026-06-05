from collections import defaultdict
from datetime import datetime, timezone
from uuid import UUID

import redis.asyncio as aioredis

from app.config import settings
from app.kafka.handlers import EVENT_HANDLERS
from app.models.notifications import Notification


def format_notification(event_type: str, event_data: dict) -> Notification | None:
    """Build a Notification from event data.

    Returns None when customer_id is missing or event_type is unknown.
    """
    template = EVENT_HANDLERS.get(event_type)
    if template is None:
        return None

    customer_id = event_data.get("customer_id")
    if not customer_id:
        return None

    message = template.format_map(defaultdict(str, event_data))

    return Notification(
        event_id=UUID(str(event_data.get("event_id", ""))),
        order_id=UUID(str(event_data.get("order_id", ""))),
        customer_id=UUID(str(customer_id)),
        event_type=event_type,
        message=message,
        timestamp=event_data.get("timestamp", datetime.now(timezone.utc)),
    )


async def dispatch_notification(
    customer_id: str, notification: Notification, redis_client: aioredis.Redis
) -> None:
    """Publish notification to Redis pub/sub channel and store in capped history list."""
    channel = f"notifications:{customer_id}"
    payload = notification.model_dump_json()

    await redis_client.publish(channel, payload)

    history_key = f"notifications:history:{customer_id}"
    await redis_client.lpush(history_key, payload)
    await redis_client.ltrim(history_key, 0, settings.notification_history_max - 1)
