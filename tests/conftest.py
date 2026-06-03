from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(name="fake_redis")
def fixture_fake_redis():
    """Create a fakeredis instance for testing."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture(autouse=True)
def _mock_redis_lifecycle(fake_redis):
    """Replace Redis lifecycle and get_redis with fakeredis for all tests."""
    with (
        patch("app.main.start_redis", new_callable=AsyncMock),
        patch("app.main.stop_redis", new_callable=AsyncMock),
        patch("app.redis_client._redis", fake_redis),
    ):
        yield


@pytest.fixture(name="client")
def fixture_client():
    """HTTP test client with mocked Redis."""
    with TestClient(app) as client:
        yield client
