import json

from app.kafka.consumer import handle_event

CUSTOMER_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
EVENT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ORDER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


async def test_handle_event_dispatches_notification(fake_redis):
    """Valid OrderCreated event produces a notification in Redis history."""
    event = {
        "event_type": "OrderCreated",
        "event_id": EVENT_ID,
        "order_id": ORDER_ID,
        "customer_id": CUSTOMER_ID,
        "restaurant_id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
        "amount": 25.50,
        "delivery_address": "123 Main St",
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    history = await fake_redis.lrange(f"notifications:history:{CUSTOMER_ID}", 0, -1)
    assert len(history) == 1

    notification = json.loads(history[0])
    assert notification["event_type"] == "OrderCreated"
    assert notification["message"] == "Your order has been placed"


async def test_handle_event_skips_duplicate(fake_redis):
    """Duplicate event_id does not produce a second notification."""
    event = {
        "event_type": "OrderCreated",
        "event_id": EVENT_ID,
        "order_id": ORDER_ID,
        "customer_id": CUSTOMER_ID,
        "amount": 25.50,
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)
    await handle_event(event, fake_redis)

    history = await fake_redis.lrange(f"notifications:history:{CUSTOMER_ID}", 0, -1)
    assert len(history) == 1


async def test_handle_event_skips_missing_event_id(fake_redis):
    """Event without event_id is silently skipped."""
    event = {"event_type": "OrderCreated", "customer_id": CUSTOMER_ID, "timestamp": "2026-06-03T10:00:00Z"}
    await handle_event(event, fake_redis)

    keys = await fake_redis.keys("notifications:history:*")
    assert len(keys) == 0


async def test_handle_event_skips_missing_customer_id(fake_redis):
    """Event without customer_id is skipped (format_notification returns None)."""
    event = {
        "event_type": "OrderCreated",
        "event_id": EVENT_ID,
        "order_id": ORDER_ID,
        "amount": 10.0,
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    keys = await fake_redis.keys("notifications:history:*")
    assert len(keys) == 0


async def test_handle_event_unknown_event_type_skipped(fake_redis):
    """Unknown event_type is skipped."""
    event = {
        "event_type": "UnknownEvent",
        "event_id": EVENT_ID,
        "order_id": ORDER_ID,
        "customer_id": CUSTOMER_ID,
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    keys = await fake_redis.keys("notifications:history:*")
    assert len(keys) == 0


async def test_handle_event_payment_failed_includes_reason(fake_redis):
    """PaymentFailed notification includes the failure reason."""
    event = {
        "event_type": "PaymentFailed",
        "event_id": EVENT_ID,
        "order_id": ORDER_ID,
        "customer_id": CUSTOMER_ID,
        "reason": "Insufficient funds",
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    history = await fake_redis.lrange(f"notifications:history:{CUSTOMER_ID}", 0, -1)
    notification = json.loads(history[0])
    assert "Insufficient funds" in notification["message"]
