from importlib.metadata import PackageNotFoundError, version

from pydantic_settings import BaseSettings, SettingsConfigDict


def _get_default_service_version() -> str:
    """Derive the default service version from installed package metadata."""
    try:
        return version("notification-service")
    except PackageNotFoundError:
        return "0.1.0"


class Settings(BaseSettings):
    """Configuration loaded from environment variables with sensible defaults."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "notification-service"
    service_version: str = _get_default_service_version()

    redis_url: str = "redis://localhost:6379/0"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_orders: str = "orders"
    kafka_topic_payments: str = "payments"
    kafka_topic_restaurants: str = "restaurants"
    kafka_topic_couriers: str = "couriers"
    kafka_topic_deliveries: str = "deliveries"
    kafka_group_id: str = "notification-service-group"

    notification_history_max: int = 50
    idempotency_ttl_seconds: int = 86400


settings = Settings()
