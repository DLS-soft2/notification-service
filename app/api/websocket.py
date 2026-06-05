import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.redis_client import get_redis
from app.service.notification_store import get_recent_notifications
from app.service.websocket_service import subscribe_to_notifications

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/api/v1/ws/{customer_id}")
async def websocket_endpoint(websocket: WebSocket, customer_id: str):
    """Stream live notifications to a customer via WebSocket.

    1. Accept connection and replay recent history.
    2. Subscribe to Redis pub/sub channel for this customer.
    3. Forward messages until the client disconnects.
    """
    await websocket.accept()

    try:
        shared_redis = get_redis()
        history = await get_recent_notifications(shared_redis, customer_id)
        for notification in history:
            await websocket.send_json(notification)

        async with subscribe_to_notifications(customer_id) as pubsub:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                await websocket.send_text(message["data"])

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for customer %s", customer_id)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("WebSocket error for customer %s", customer_id)
