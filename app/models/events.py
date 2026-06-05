from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class OrderCreated(BaseModel):
    """Event consumed from the 'orders' topic when a new order is placed."""

    event_type: str = "OrderCreated"
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: UUID
    restaurant_id: UUID | None = None
    amount: float
    delivery_address: str | None = None
    timestamp: datetime


class PaymentAuthorized(BaseModel):
    """Payment was successfully authorized for an order."""

    event_type: str = "PaymentAuthorized"
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: UUID | None = None
    payment_id: UUID | None = None
    amount: float
    timestamp: datetime


class PaymentFailed(BaseModel):
    """Payment failed for an order."""

    event_type: str = "PaymentFailed"
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: UUID | None = None
    reason: str
    timestamp: datetime


class RestaurantAccepted(BaseModel):
    """Restaurant accepted the order and started preparing."""

    event_type: str = "RestaurantAccepted"
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: UUID | None = None
    estimated_prep_time: int | None = None
    timestamp: datetime


class CourierAssigned(BaseModel):
    """A courier has been assigned to pick up and deliver the order."""

    event_type: str = "CourierAssigned"
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: UUID | None = None
    courier_id: UUID | None = None
    timestamp: datetime


class DeliveryCompleted(BaseModel):
    """The order has been delivered to the customer."""

    event_type: str = "DeliveryCompleted"
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: UUID | None = None
    timestamp: datetime
