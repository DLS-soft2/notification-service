from fastapi import APIRouter, HTTPException

from app.redis_client import get_redis
from app.service.receipt_service import get_delivery_receipt

router = APIRouter(prefix="/api/v1", tags=["receipts"])


@router.get("/receipts/{order_id}")
async def read_receipt(order_id: str):
    """Return the delivery receipt for an order, or 404 if not found."""
    redis_client = get_redis()
    receipt = await get_delivery_receipt(order_id, redis_client)
    if receipt is None:
        raise HTTPException(status_code=404, detail="Receipt not found")
    return receipt
