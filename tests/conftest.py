from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from fakeredis import FakeAsyncRedis


@pytest.fixture()
def client():
    """TestClient with Redis mocked via fakeredis."""
    fake_redis = FakeAsyncRedis()

    with patch("app.redis_client.aioredis") as mock_aioredis:
        mock_aioredis.from_url.return_value = fake_redis

        from app.main import app  # pylint: disable=import-outside-toplevel
        with TestClient(app) as test_client:
            yield test_client
