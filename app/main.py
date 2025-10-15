import logging
from fastapi import FastAPI


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Create the FastAPI app instance
app = FastAPI(
    title="Sagepick Core",
    description="Sagepick Core Backend API and Services with Automated Cron Jobs",
    version="1.0.0"
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
