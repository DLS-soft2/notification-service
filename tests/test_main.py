def test_health(client):
    """GET /health returns 200 with status healthy."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_root(client):
    """GET / returns 200 with service name."""
    response = client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["service"] == "notification-service"
    assert "version" in body
