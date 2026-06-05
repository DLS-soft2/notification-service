# notification-service

Real-time notification service for the food delivery platform. Consumes saga events from Kafka, deduplicates via Redis, and pushes notifications to clients over WebSocket.

## Architecture

```
Kafka (5 topics) -> Consumer -> Idempotency Check (Redis SETNX) -> Format Notification -> Redis Pub/Sub -> WebSocket
```

## Consumed Kafka Topics

| Topic | Events |
|-------|--------|
| `orders` | OrderCreated |
| `payments` | PaymentAuthorized, PaymentFailed |
| `restaurants` | RestaurantAccepted |
| `couriers` | CourierAssigned |
| `deliveries` | DeliveryCompleted |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| WebSocket | `/api/v1/ws/{customer_id}` | Real-time notifications with history replay |
| GET | `/health` | Health check |
| GET | `/` | Service info |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_ORDERS` | `orders` | Orders topic name |
| `KAFKA_TOPIC_PAYMENTS` | `payments` | Payments topic name |
| `KAFKA_TOPIC_RESTAURANTS` | `restaurants` | Restaurants topic name |
| `KAFKA_TOPIC_COURIERS` | `couriers` | Couriers topic name |
| `KAFKA_TOPIC_DELIVERIES` | `deliveries` | Deliveries topic name |
| `KAFKA_GROUP_ID` | `notification-service-group` | Consumer group ID |
| `NOTIFICATION_HISTORY_MAX` | `50` | Max notifications stored per customer |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | Idempotency key TTL (24h) |

## Running Locally

```bash
poetry install
poetry run uvicorn app.main:app --reload
```

## Running Tests

```bash
poetry run pytest --tb=short -v
```

## Docker

```bash
docker compose up --build
```
