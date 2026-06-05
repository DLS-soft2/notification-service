import json

import pytest

from app.kafka.consumer import handle_event, is_duplicate
from app.service.notification_service import format_notification


@pytest.fixture(name="order_created_event")
def fixture_order_created_event():
    """Valid OrderCreated event payload."""
    return {
        "event_type": "OrderCreated",
        "event_id": "evt-001",
        "order_id": "ord-001",
        "customer_id": "cust-001",
        "restaurant_id": "rest-001",
        "amount": 25.50,
        "delivery_address": "123 Main St",
        "timestamp": "2026-06-03T10:00:00Z",
    }


# --- Idempotency tests ---


async def test_is_duplicate_returns_false_for_new_event(fake_redis):
    """First call for a new event_id should return False (not a duplicate)."""
    result = await is_duplicate("new-event-id", fake_redis)
    assert result is False


async def test_is_duplicate_returns_true_on_second_call(fake_redis):
    """Second call with same event_id should return True (duplicate)."""
    first = await is_duplicate("evt-dup", fake_redis)
    second = await is_duplicate("evt-dup", fake_redis)
    assert first is False
    assert second is True


async def test_is_duplicate_sets_idempotency_key(fake_redis):
    """is_duplicate sets a key with the format 'idempotency:{event_id}'."""
    await is_duplicate("evt-fmt", fake_redis)
    exists = await fake_redis.exists("idempotency:evt-fmt")
    assert exists


# --- handle_event tests ---


async def test_handle_event_dispatches_notification(fake_redis, order_created_event):
    """Valid OrderCreated event produces a notification in Redis history."""
    await handle_event(order_created_event, fake_redis)

    history = await fake_redis.lrange("notification_history:cust-001", 0, -1)
    assert len(history) == 1

    notification = json.loads(history[0])
    assert notification["customer_id"] == "cust-001"
    assert notification["event_type"] == "OrderCreated"
    assert notification["message"] == "Your order has been placed"
    assert notification["event_id"] == "evt-001"


async def test_handle_event_skips_duplicate(fake_redis, order_created_event):
    """Duplicate event_id does not produce a second notification."""
    await handle_event(order_created_event, fake_redis)
    await handle_event(order_created_event, fake_redis)

    history = await fake_redis.lrange("notification_history:cust-001", 0, -1)
    assert len(history) == 1


async def test_handle_event_skips_missing_event_id(fake_redis):
    """Event without event_id is silently skipped."""
    event = {"event_type": "OrderCreated", "customer_id": "cust-001", "timestamp": "2026-06-03T10:00:00Z"}
    await handle_event(event, fake_redis)

    history = await fake_redis.lrange("notification_history:cust-001", 0, -1)
    assert len(history) == 0


async def test_handle_event_skips_missing_customer_id(fake_redis):
    """Event without customer_id is marked processed but no notification dispatched."""
    event = {
        "event_type": "OrderCreated",
        "event_id": "evt-no-cust",
        "order_id": "ord-001",
        "amount": 10.0,
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    # is_duplicate claimed the key atomically during handle_event
    exists = await fake_redis.exists("idempotency:evt-no-cust")
    assert exists

    keys = await fake_redis.keys("notification_history:*")
    assert len(keys) == 0


async def test_handle_event_payment_failed_includes_reason(fake_redis):
    """PaymentFailed notification includes the failure reason."""
    event = {
        "event_type": "PaymentFailed",
        "event_id": "evt-pf-001",
        "order_id": "ord-002",
        "customer_id": "cust-002",
        "reason": "Insufficient funds",
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    history = await fake_redis.lrange("notification_history:cust-002", 0, -1)
    notification = json.loads(history[0])
    assert "Insufficient funds" in notification["message"]


async def test_handle_event_restaurant_accepted_includes_prep_time(fake_redis):
    """RestaurantAccepted notification includes estimated prep time."""
    event = {
        "event_type": "RestaurantAccepted",
        "event_id": "evt-ra-001",
        "order_id": "ord-003",
        "customer_id": "cust-003",
        "estimated_prep_time": 15,
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    history = await fake_redis.lrange("notification_history:cust-003", 0, -1)
    notification = json.loads(history[0])
    assert "~15 min" in notification["message"]


async def test_handle_event_unknown_event_type_skipped(fake_redis):
    """Unknown event_type is skipped and marked processed."""
    event = {
        "event_type": "UnknownEvent",
        "event_id": "evt-unk-001",
        "order_id": "ord-004",
        "customer_id": "cust-004",
        "timestamp": "2026-06-03T10:00:00Z",
    }
    await handle_event(event, fake_redis)

    exists = await fake_redis.exists("idempotency:evt-unk-001")
    assert exists

    keys = await fake_redis.keys("notification_history:*")
    assert len(keys) == 0


# --- format_notification tests ---


def test_format_order_created():
    """OrderCreated produces the correct message."""
    result = format_notification("OrderCreated", {})
    assert result == "Your order has been placed"


def test_format_payment_authorized():
    """PaymentAuthorized produces the correct message."""
    result = format_notification("PaymentAuthorized", {})
    assert result == "Payment confirmed — restaurant notified"


def test_format_payment_failed():
    """PaymentFailed includes the reason."""
    result = format_notification("PaymentFailed", {"reason": "Card declined"})
    assert result == "Payment failed: Card declined"


def test_format_restaurant_accepted():
    """RestaurantAccepted includes estimated prep time."""
    result = format_notification("RestaurantAccepted", {"estimated_prep_time": 20})
    assert result == "Restaurant is preparing your order (~20 min)"


def test_format_courier_assigned():
    """CourierAssigned produces the correct message."""
    result = format_notification("CourierAssigned", {})
    assert result == "A courier is on the way to pick up your order"


def test_format_delivery_completed():
    """DeliveryCompleted produces the correct message."""
    result = format_notification("DeliveryCompleted", {})
    assert result == "Your order has been delivered!"


def test_format_unknown_event_type():
    """Unknown event type returns None."""
    result = format_notification("SomethingWeird", {})
    assert result is None
