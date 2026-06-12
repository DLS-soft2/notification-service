from app.models.notifications import Notification
from app.service.notification_service import format_notification

EVENT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ORDER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
CUSTOMER_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
TIMESTAMP = "2026-06-03T10:00:00Z"


def _base_event(**overrides: object) -> dict:
    base = {"event_id": EVENT_ID, "order_id": ORDER_ID, "customer_id": CUSTOMER_ID, "timestamp": TIMESTAMP}
    base.update(overrides)
    return base


def test_format_order_created():
    """OrderCreated produces a Notification with the correct message."""
    result = format_notification("OrderCreated", _base_event())
    assert isinstance(result, Notification)
    assert result.message == "Your order has been placed"


def test_format_payment_authorized():
    """PaymentAuthorized produces the correct message."""
    result = format_notification("PaymentAuthorized", _base_event())
    assert isinstance(result, Notification)
    assert result.message == "Payment confirmed"


def test_format_payment_failed():
    """PaymentFailed includes the reason."""
    result = format_notification("PaymentFailed", _base_event(reason="Card declined"))
    assert isinstance(result, Notification)
    assert result.message == "Payment failed: Card declined"


def test_format_restaurant_accepted():
    """RestaurantAccepted includes estimated prep time."""
    result = format_notification("RestaurantAccepted", _base_event(estimated_prep_time=20))
    assert isinstance(result, Notification)
    assert result.message == "Restaurant is preparing your order (~20 min)"


def test_format_courier_assigned():
    """CourierAssigned includes the courier name in the message."""
    result = format_notification("CourierAssigned", _base_event(courier_name="Ox"))
    assert isinstance(result, Notification)
    assert "Ox" in result.message
    assert result.message == "Courier Ox is on the way!"


def test_format_delivery_completed():
    """DeliveryCompleted produces the correct message."""
    result = format_notification("DeliveryCompleted", _base_event())
    assert isinstance(result, Notification)
    assert result.message == "Order delivered!"


def test_format_unknown_event_type():
    """Unknown event type returns None."""
    result = format_notification("SomethingWeird", _base_event())
    assert result is None


def test_format_missing_customer_id():
    """Missing customer_id returns None."""
    event = {"event_id": EVENT_ID, "order_id": ORDER_ID, "timestamp": TIMESTAMP}
    result = format_notification("OrderCreated", event)
    assert result is None


def test_format_missing_order_id():
    """Missing order_id returns None."""
    event = {"event_id": EVENT_ID, "customer_id": CUSTOMER_ID, "timestamp": TIMESTAMP}
    result = format_notification("OrderCreated", event)
    assert result is None


def test_format_missing_event_id():
    """Missing event_id returns None."""
    event = {"order_id": ORDER_ID, "customer_id": CUSTOMER_ID, "timestamp": TIMESTAMP}
    result = format_notification("OrderCreated", event)
    assert result is None


def test_format_restaurant_rejected():
    """RestaurantRejected includes the rejection reason."""
    result = format_notification("RestaurantRejected", _base_event(reason="Kitchen closed"))
    assert isinstance(result, Notification)
    assert "Kitchen closed" in result.message


def test_format_courier_assignment_failed():
    """CourierAssignmentFailed includes the failure reason."""
    result = format_notification("CourierAssignmentFailed", _base_event(reason="No available couriers"))
    assert isinstance(result, Notification)
    assert "No available couriers" in result.message


def test_format_payment_refunded():
    """PaymentRefunded includes refund information."""
    result = format_notification("PaymentRefunded", _base_event(reason="Order cancelled"))
    assert isinstance(result, Notification)
    assert "refund" in result.message.lower()
