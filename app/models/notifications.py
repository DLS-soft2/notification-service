from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Notification(BaseModel):
    """A notification dispatched to a customer."""

    id: UUID = Field(default_factory=uuid4)
    event_id: UUID
    order_id: UUID
    customer_id: UUID
    event_type: str
    message: str
    timestamp: datetime
