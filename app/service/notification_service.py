from collections import defaultdict

import redis.asyncio as aioredis

from app.config import settings
from app.models.notifications import Notification

EVENT_MESSAGES: dict[str, str] = {
    "OrderCreated": "Your order has been placed",
    "PaymentAuthorized": "Payment confirmed — restaurant notified",
    "PaymentFailed": "Payment failed: {reason}",
    "RestaurantAccepted": "Restaurant is preparing your order (~{estimated_prep_time} min)",
    "CourierAssigned": "A courier is on the way to pick up your order",
    "DeliveryCompleted": "Your order has been delivered!",
}


def format_notification(event_type: str, event_data: dict) -> str | None:
    """Format a human-readable notification message from event data.

    Returns None for unknown event types.
    """
    template = EVENT_MESSAGES.get(event_type)
    if template is None:
        return None
    return template.format_map(defaultdict(str, event_data))


async def dispatch_notification(customer_id: str, notification: Notification, redis_client: aioredis.Redis) -> None:
    """Publish notification to Redis pub/sub channel and store in capped history list."""
    channel = f"notifications:{customer_id}"
    payload = notification.model_dump_json()

    await redis_client.publish(channel, payload)

    history_key = f"notification_history:{customer_id}"
    await redis_client.lpush(history_key, payload)
    await redis_client.ltrim(history_key, 0, settings.notification_history_max - 1)
