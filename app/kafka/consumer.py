import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from aiokafka import AIOKafkaConsumer
import redis.asyncio as aioredis

from app.config import settings
from app.models.notifications import Notification
from app.redis_client import get_redis
from app.service.notification_service import dispatch_notification, format_notification

logger = logging.getLogger(__name__)


async def is_duplicate(event_id: str, redis_client: aioredis.Redis) -> bool:
    """Atomically check and mark an event as processed. Returns True if already processed."""
    key = f"idempotency:{event_id}"
    result = await redis_client.set(key, "1", nx=True, ex=settings.idempotency_ttl_seconds)
    # result is True if key was SET (new event), None if key already existed (duplicate)
    return result is None


async def handle_event(message_value: dict, redis_client: aioredis.Redis) -> None:
    """Process a single Kafka event.

    1. Extract event_id and event_type — skip if missing.
    2. Atomic idempotency check via is_duplicate — skip if already processed.
    3. Extract customer_id — skip with warning if missing.
    4. Format notification message — skip if unknown event type.
    5. Dispatch to Redis pub/sub + history.
    """
    event_id = message_value.get("event_id")
    event_type = message_value.get("event_type")
    customer_id = message_value.get("customer_id")
    order_id = message_value.get("order_id")

    if not event_id or not event_type:
        logger.warning("Missing event_id or event_type — skipping")
        return

    if await is_duplicate(event_id, redis_client):
        logger.info("Duplicate event %s — skipping", event_id)
        return

    if not customer_id:
        logger.warning("No customer_id in %s event %s — skipping notification", event_type, event_id)
        return

    message = format_notification(event_type, message_value)
    if message is None:
        logger.warning("Unknown event_type %s — skipping", event_type)
        return

    notification = Notification(
        notification_id=str(uuid4()),
        event_id=event_id,
        order_id=order_id or "",
        customer_id=customer_id,
        event_type=event_type,
        message=message,
        timestamp=message_value.get("timestamp", datetime.now(timezone.utc).isoformat()),
    )

    await dispatch_notification(customer_id, notification, redis_client)
    logger.info("Notification sent for %s event %s to customer %s", event_type, event_id, customer_id)


async def start_consumer() -> None:
    """Start the multi-topic Kafka consumer loop.

    Subscribes to orders, payments, restaurants, couriers, and deliveries topics.
    Uses group_id from settings for horizontal scaling.
    Retries connection up to 10 times with 3-second intervals.
    """
    topics = [
        settings.kafka_topic_orders,
        settings.kafka_topic_payments,
        settings.kafka_topic_restaurants,
        settings.kafka_topic_couriers,
        settings.kafka_topic_deliveries,
    ]

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        auto_offset_reset="earliest",
    )

    for attempt in range(1, 11):
        try:
            await consumer.start()
            logger.info("Kafka consumer started — listening on %s", topics)
            break
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Kafka not ready for consumer (attempt %d/10): %s", attempt, exc)
            if attempt == 10:
                raise
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            try:
                value = json.loads(msg.value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Skipping invalid message at offset %d: %s", msg.offset, exc)
                continue

            logger.info(
                "Received message from topic '%s' partition %d offset %d",
                msg.topic, msg.partition, msg.offset,
            )

            redis_client = get_redis()
            await handle_event(value, redis_client)

    except asyncio.CancelledError:
        logger.info("Consumer task was cancelled")
    except Exception as exc:
        logger.error("Consumer crashed with error: %s", exc, exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")
