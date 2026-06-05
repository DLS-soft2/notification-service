import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
import redis.asyncio as aioredis

from app.config import settings
from app.redis_client import get_redis
from app.service.idempotency import is_duplicate
from app.service.notification_service import dispatch_notification, format_notification

logger = logging.getLogger(__name__)


async def handle_event(message_value: dict, redis_client: aioredis.Redis) -> None:
    """Process a single Kafka event.

    1. Extract event_id and event_type — skip if missing.
    2. Atomic idempotency check via is_duplicate — skip if already processed.
    3. Format notification (returns None if customer_id missing or unknown event_type).
    4. Dispatch to Redis pub/sub + history.
    """
    event_id = message_value.get("event_id")
    event_type = message_value.get("event_type")

    if not event_id or not event_type:
        logger.warning("Missing event_id or event_type — skipping")
        return

    if await is_duplicate(redis_client, str(event_id)):
        logger.info("Duplicate event %s — skipping", event_id)
        return

    notification = format_notification(event_type, message_value)
    if notification is None:
        logger.warning("Skipping event %s — unknown type or missing customer_id", event_id)
        return

    customer_id = str(notification.customer_id)
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
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Consumer crashed with error: %s", exc, exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")
