import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.api.health import router as health_router
from app.kafka.consumer import start_consumer
from app.redis_client import start_redis, stop_redis

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Start Redis and Kafka consumer on startup; cancel and close on shutdown."""
    await start_redis()

    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Kafka consumer background task started")

    yield

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        logger.info("Kafka consumer task cancelled")

    await stop_redis()


app = FastAPI(
    title="Notification Service",
    description="Kafka-driven notification service for the DLS-2 food delivery platform",
    version=settings.service_version,
    lifespan=lifespan,
)

app.include_router(health_router)


@app.get("/")
def root():
    """Service info endpoint."""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
    }
