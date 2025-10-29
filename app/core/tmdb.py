import logging
from typing import Optional
from app.services.tmdb_client.client import TMDBClient

logger = logging.getLogger(__name__)


class TMDBManager:
    _instance: Optional[TMDBClient] = None
    
    @classmethod
    async def get_client(cls) -> TMDBClient:
        """Get the singleton TMDB client instance."""
        if cls._instance is None:
            logger.info("Creating TMDB client singleton instance")
            cls._instance = TMDBClient()
        return cls._instance
    
    @classmethod
    async def close(cls) -> None:
        """Close the TMDB client and cleanup resources."""
        if cls._instance is not None:
            logger.info("Closing TMDB client singleton instance")
            await cls._instance.close()
            cls._instance = None
    
    @classmethod
    def is_initialized(cls) -> bool:
        return cls._instance is not None


# Singleton access function
async def get_tmdb_client() -> TMDBClient:
    return await TMDBManager.get_client()


# Cleanup function for application shutdown
async def close_tmdb_client() -> None:
    await TMDBManager.close()