"""Health check endpoint for monitoring and container orchestration."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    """Liveness probe."""
    return {"status": "healthy"}
