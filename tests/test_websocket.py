import json
import threading
import time
from unittest.mock import patch

import fakeredis
import fakeredis.aioredis
from fastapi.testclient import TestClient

from app.main import app

CUSTOMER_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"

SAMPLE_NOTIFICATION = {
    "id": "11111111-1111-1111-1111-111111111111",
    "event_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "customer_id": CUSTOMER_ID,
    "order_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "message": "Your order has been placed",
    "event_type": "OrderCreated",
    "timestamp": "2026-06-03T10:00:00Z",
}


def _make_redis_pair():
    """Return two fakeredis instances sharing the same in-memory server."""
    server = fakeredis.FakeServer()
    shared = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    dedicated = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return server, shared, dedicated


def _apply_patches(shared, dedicated):
    """Stack context managers that wire both shared and dedicated Redis fakes."""
    from unittest.mock import AsyncMock
    return (
        patch("app.main.get_redis", return_value=shared),
        patch("app.main.close_redis", new_callable=AsyncMock),
        patch("app.main.start_consumer", new_callable=AsyncMock),
        patch("app.redis_client._redis", shared),
        patch("app.api.websocket.get_redis", return_value=shared),
        patch("app.service.websocket_service.aioredis.from_url", return_value=dedicated),
    )


def test_websocket_replays_history():
    """On connect, client receives recent notification history from Redis."""
    server, shared, dedicated = _make_redis_pair()

    patches = _apply_patches(shared, dedicated)
    for p in patches:
        p.start()

    # Pre-populate history via a synchronous fakeredis client (same server)
    sync_redis = fakeredis.FakeRedis(server=server, decode_responses=True)
    history_key = f"notifications:history:{CUSTOMER_ID}"
    sync_redis.lpush(history_key, json.dumps(SAMPLE_NOTIFICATION))
    second_notification = {**SAMPLE_NOTIFICATION, "id": "22222222-2222-2222-2222-222222222222", "message": "Payment confirmed"}
    sync_redis.lpush(history_key, json.dumps(second_notification))

    try:
        with TestClient(app) as client:
            with client.websocket_connect(f"/api/v1/ws/{CUSTOMER_ID}") as ws:
                msg1 = ws.receive_json()
                assert msg1["message"] == "Payment confirmed"
                msg2 = ws.receive_json()
                assert msg2["message"] == "Your order has been placed"
                assert msg2["event_type"] == "OrderCreated"
                assert msg2["customer_id"] == CUSTOMER_ID
    finally:
        for p in patches:
            p.stop()


def test_websocket_live_notification():
    """Messages published to Redis channel are forwarded to the WebSocket client."""
    server, shared, dedicated = _make_redis_pair()

    patches = _apply_patches(shared, dedicated)
    for p in patches:
        p.start()

    publisher = fakeredis.FakeRedis(server=server, decode_responses=True)

    def publish_after_delay():
        time.sleep(0.3)
        channel = f"notifications:{CUSTOMER_ID}"
        publisher.publish(channel, json.dumps(SAMPLE_NOTIFICATION))

    try:
        with TestClient(app) as client:
            with client.websocket_connect(f"/api/v1/ws/{CUSTOMER_ID}") as ws:
                thread = threading.Thread(target=publish_after_delay)
                thread.start()
                msg = ws.receive_text()
                data = json.loads(msg)
                assert data["message"] == "Your order has been placed"
                assert data["customer_id"] == CUSTOMER_ID
                thread.join()
    finally:
        for p in patches:
            p.stop()


def test_websocket_disconnect_cleanup():
    """Disconnecting the WebSocket does not crash the server."""
    server, shared, dedicated = _make_redis_pair()

    patches = _apply_patches(shared, dedicated)
    for p in patches:
        p.start()

    try:
        with TestClient(app) as client:
            with client.websocket_connect(f"/api/v1/ws/{CUSTOMER_ID}") as ws:
                pass  # immediately exit -> disconnect

        # Server still responsive after disconnect
        with TestClient(app) as client:
            resp = client.get("/health")
            assert resp.status_code == 200
    finally:
        for p in patches:
            p.stop()
