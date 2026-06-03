from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.api.health import router as health_router
from app.redis_client import start_redis, stop_redis


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Start Redis on startup; close on shutdown. Kafka consumer added in P2."""
    await start_redis()
    yield
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
