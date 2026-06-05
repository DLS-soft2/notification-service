from importlib.metadata import PackageNotFoundError, version

from pydantic_settings import BaseSettings, SettingsConfigDict


def _get_default_service_version() -> str:
    """Derive the default service version from installed package metadata."""
    try:
        return version("notification-service")
    except PackageNotFoundError:
        return "0.1.0"


class Settings(BaseSettings):
    """Central configuration for the Notification Service."""

    model_config = SettingsConfigDict(env_file=".env")

    # Service metadata
    service_name: str = "notification-service"
    service_version: str = _get_default_service_version()

    # Redis connection
    redis_url: str = "redis://localhost:6379/0"

    # Kafka connection
    kafka_bootstrap_servers: str = "localhost:9092"

    # Kafka topics this service consumes
    kafka_topic_orders: str = "orders"
    kafka_topic_payments: str = "payments"
    kafka_topic_restaurants: str = "restaurants"
    kafka_topic_couriers: str = "couriers"
    kafka_topic_deliveries: str = "deliveries"


# Single instance used throughout the app
settings = Settings()
