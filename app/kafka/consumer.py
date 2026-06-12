import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
import redis.asyncio as aioredis

from app.config import Settings, settings
from app.redis_client import get_redis
from app.service.idempotency import is_duplicate
from app.service.notification_service import dispatch_notification, format_notification
from app.service.receipt_service import generate_delivery_receipt

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


async def _process_dispatch_batch(
    batch: dict, end_offsets: dict, reached: dict, redis_client: aioredis.Redis,
) -> None:
    """Process one batch of delivery messages into receipts and update reached tracking."""
    for tp, messages in batch.items():
        for msg in messages:
            try:
                value = json.loads(msg.value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Skipping invalid message at offset %d: %s", msg.offset, exc)
                continue

            event_id = value.get("event_id")
            if event_id and await is_duplicate(redis_client, f"dispatch:{event_id}"):
                logger.info("Duplicate dispatch event %s — skipping", event_id)
                continue

            receipt = await generate_delivery_receipt(value, redis_client)
            if receipt:
                logger.info("Delivery receipt generated for order %s", receipt["order_id"])
            else:
                logger.warning("Skipping dispatch message — missing required fields")

        if messages and messages[-1].offset + 1 >= end_offsets[tp]:
            reached[tp] = True


async def _drain_partitions(consumer: AIOKafkaConsumer, end_offsets: dict, reached: dict) -> None:
    """Drain all partitions up to their end offsets with a 60s safety timeout."""
    deadline = asyncio.get_running_loop().time() + 60
    redis_client = get_redis()

    while not all(reached.values()):
        if asyncio.get_running_loop().time() > deadline:
            logger.warning("Dispatch job hit 60s safety timeout — exiting")
            break

        batch = await asyncio.wait_for(
            consumer.getmany(timeout_ms=2000, max_records=100),
            timeout=10,
        )

        await _process_dispatch_batch(batch, end_offsets, reached, redis_client)

        if not batch:
            for tp in reached:
                position = await consumer.position(tp)
                if position >= end_offsets[tp]:
                    reached[tp] = True


async def run_dispatch_job(dispatch_settings: Settings) -> None:
    """Consume pending delivery events and generate delivery receipts, then exit.

    Designed for KEDA ScaledJob: drains all messages up to the current
    end offsets, generates a delivery receipt for each, then stops.
    A 60-second safety timeout prevents the job from hanging.
    """
    consumer = AIOKafkaConsumer(
        dispatch_settings.kafka_topic_deliveries,
        bootstrap_servers=dispatch_settings.kafka_bootstrap_servers,
        group_id=dispatch_settings.kafka_group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    for attempt in range(1, 11):
        try:
            await consumer.start()
            logger.info("Dispatch consumer started on topic '%s'", dispatch_settings.kafka_topic_deliveries)
            break
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Kafka not ready for dispatch consumer (attempt %d/10): %s", attempt, exc)
            if attempt == 10:
                raise
            await asyncio.sleep(3)

    try:
        partitions = consumer.assignment()
        if not partitions:
            await asyncio.sleep(1)
            partitions = consumer.assignment()

        if not partitions:
            raise RuntimeError("Partition assignment still empty after wait — rebalance failed")

        end_offsets = await consumer.end_offsets(partitions)
        if all(end_offsets[tp] == 0 for tp in partitions):
            logger.info("No messages to process — exiting dispatch job")
            return

        reached = {tp: False for tp in partitions}
        await _drain_partitions(consumer, end_offsets, reached)

        await consumer.commit()
        logger.info("Dispatch job completed — all partitions drained")
    finally:
        await consumer.stop()
        logger.info("Dispatch consumer stopped")
