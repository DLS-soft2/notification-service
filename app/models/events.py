from pydantic import BaseModel


class OrderCreated(BaseModel):
    """Event consumed from the 'orders' topic when a new order is placed."""

    event_type: str = "OrderCreated"
    event_id: str
    order_id: str
    customer_id: str
    restaurant_id: str | None = None
    amount: float
    delivery_address: str | None = None
    timestamp: str


class PaymentAuthorized(BaseModel):
    """Payment was successfully authorized for an order."""

    event_type: str = "PaymentAuthorized"
    event_id: str
    order_id: str
    customer_id: str | None = None
    payment_id: str | None = None
    amount: float
    timestamp: str


class PaymentFailed(BaseModel):
    """Payment failed for an order."""

    event_type: str = "PaymentFailed"
    event_id: str
    order_id: str
    customer_id: str | None = None
    reason: str
    timestamp: str


class RestaurantAccepted(BaseModel):
    """Restaurant accepted the order and started preparing."""

    event_type: str = "RestaurantAccepted"
    event_id: str
    order_id: str
    customer_id: str | None = None
    estimated_prep_time: int | None = None
    timestamp: str


class CourierAssigned(BaseModel):
    """A courier has been assigned to pick up and deliver the order."""

    event_type: str = "CourierAssigned"
    event_id: str
    order_id: str
    customer_id: str | None = None
    courier_id: str | None = None
    timestamp: str


class DeliveryCompleted(BaseModel):
    """The order has been delivered to the customer."""

    event_type: str = "DeliveryCompleted"
    event_id: str
    order_id: str
    customer_id: str | None = None
    timestamp: str
