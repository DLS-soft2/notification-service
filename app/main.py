import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.redis_client import get_redis, close_redis

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage Redis connection lifecycle. Kafka consumer added in P2."""
    get_redis()
    logger.info("Redis client initialised")

    yield

    await close_redis()
    logger.info("Redis client closed")


app = FastAPI(
    title="Notification Service",
    description="Kafka consumer that pushes real-time notifications via WebSocket",
    version=settings.service_version,
    lifespan=lifespan,
)


@app.get("/")
def root() -> dict:
    """Service info endpoint."""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
    }


@app.get("/health")
def health() -> dict:
    """Health check for monitoring and container orchestration."""
    return {"status": "healthy"}
