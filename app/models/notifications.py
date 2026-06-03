from pydantic import BaseModel


class Notification(BaseModel):
    """A notification dispatched to a customer."""

    notification_id: str
    event_id: str
    order_id: str
    customer_id: str
    event_type: str
    message: str
    timestamp: str
