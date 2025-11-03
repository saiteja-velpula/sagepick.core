from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.scheduler import job_scheduler
from app.core.redis import redis_client
from app.core.tmdb import close_tmdb_client
from app.core.exceptions import register_exception_handlers
from app.core.middleware import CorrelationIdMiddleware
from app.core.logging import setup_logging, get_structured_logger
from app.api import api_router
from app import __version__ as app_version

# Setup structured logging
setup_logging(use_json=False, correlation_id_in_format=True)
logger = get_structured_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting SAGEPICK Core application...")

    try:
        # Initialize Redis
        await redis_client.initialize()
        logger.info("Redis client initialized")

        # Start job scheduler
        await job_scheduler.start()
        logger.info("Job scheduler started")

        yield

    finally:
        # Shutdown
        logger.info("Shutting down SAGEPICK Core application...")

        # Stop job scheduler
        await job_scheduler.stop()
        logger.info("Job scheduler stopped")

        # Close TMDB client singleton
        await close_tmdb_client()
        logger.info("TMDB client closed")

        # Close Redis connection
        await redis_client.close()
        logger.info("Redis client closed")


# Create the FastAPI app instance
app = FastAPI(
    title="Sagepick Core",
    description="Sagepick Core Backend API and Services with Automated Cron Jobs",
    version=app_version,
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(CorrelationIdMiddleware)

# Register exception handlers
register_exception_handlers(app)

app.include_router(api_router)


# Root endpoint
@app.get("/")
def read_root():
    return {
        "name": "Sagepick Core Backend!",
        "version": app_version,
        "description": "Movie recommendation system with automated TMDB data synchronization",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
