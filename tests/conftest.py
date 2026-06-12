from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(name="fake_redis")
def fixture_fake_redis():
    """Create a fakeredis instance shared across a single test."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True, connected=True)


@pytest.fixture(autouse=True)
def _mock_redis_lifecycle(fake_redis):
    """Replace Redis lifecycle, Kafka consumer, and get_redis with fakes for all tests."""
    with (
        patch("app.main.get_redis", return_value=fake_redis),
        patch("app.main.close_redis", new_callable=AsyncMock),
        patch("app.main.start_consumer", new_callable=AsyncMock),
        patch("app.redis_client._redis", fake_redis),
        patch("app.api.websocket.get_redis", return_value=fake_redis),
        patch("app.api.receipts.get_redis", return_value=fake_redis),
    ):
        yield


@pytest.fixture(name="client")
def fixture_client():
    """HTTP test client with mocked Redis and Kafka."""
    with TestClient(app) as client:
        yield client
