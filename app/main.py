import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.scheduler import job_scheduler
from app.core.redis import redis_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        
        # Close Redis connection
        await redis_client.close()
        logger.info("Redis client closed")


# Create the FastAPI app instance
app = FastAPI(
    title="Sagepick Core",
    description="Sagepick Core Backend API and Services with Automated Cron Jobs",
    version="1.0.0",
    lifespan=lifespan
)

# Root endpoint
@app.get("/")
def read_root():
    return {
        "name": "Sagepick Core Backend!",
        "version": "1.0.0",
        "description": "Movie recommendation system with automated TMDB data synchronization"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
