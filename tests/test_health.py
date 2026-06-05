def test_health_returns_200(client):
    """GET /health returns HTTP 200."""
    response = client.get("/health")
    assert response.status_code == 200


def test_health_status_healthy(client):
    """GET /health body contains status 'healthy'."""
    response = client.get("/health")
    assert response.json()["status"] == "healthy"


def test_root_returns_200(client):
    """GET / returns HTTP 200."""
    response = client.get("/")
    assert response.status_code == 200
