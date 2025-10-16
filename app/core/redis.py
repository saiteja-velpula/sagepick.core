import redis.asyncio as redis
from typing import Optional
import json
import logging
from contextlib import asynccontextmanager

from app.core.settings import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis client for job management and movie ID conflict resolution."""
    
    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize Redis connection."""
        if not self._initialized:
            try:
                self.redis = redis.from_url(
                    settings.REDIS_URL,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=20,
                    retry_on_timeout=True
                )
                # Test connection
                await self.redis.ping()
                self._initialized = True
                logger.info("Redis connection initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Redis connection: {e}")
                raise
    
    async def close(self):
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            self._initialized = False
            logger.info("Redis connection closed")
    
    # Movie ID Locking for conflict resolution
    async def acquire_movie_lock(self, movie_id: int, timeout: int = 300) -> bool:
        """
        Acquire lock for a movie ID to prevent concurrent processing.
        Returns True if lock acquired, False otherwise.
        """
        if not self.redis:
            return False
        
        lock_key = f"movie_lock:{movie_id}"
        try:
            # Use SET with NX (only if not exists) and EX (expiration)
            result = await self.redis.set(lock_key, "locked", nx=True, ex=timeout)
            return result is True
        except Exception as e:
            logger.error(f"Failed to acquire movie lock for {movie_id}: {e}")
            return False
    
    async def release_movie_lock(self, movie_id: int) -> bool:
        """Release lock for a movie ID."""
        if not self.redis:
            return False
        
        lock_key = f"movie_lock:{movie_id}"
        try:
            result = await self.redis.delete(lock_key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to release movie lock for {movie_id}: {e}")
            return False
    
    async def extend_movie_lock(self, movie_id: int, timeout: int = 300) -> bool:
        """Extend the expiration time of a movie lock."""
        if not self.redis:
            return False
        
        lock_key = f"movie_lock:{movie_id}"
        try:
            result = await self.redis.expire(lock_key, timeout)
            return result
        except Exception as e:
            logger.error(f"Failed to extend movie lock for {movie_id}: {e}")
            return False
    
    # Job state management
    async def set_job_state(self, job_type: str, state: dict, ttl: int = 3600) -> bool:
        """Set job state (like current page number for discovery job)."""
        if not self.redis:
            return False
        
        key = f"job_state:{job_type}"
        try:
            await self.redis.setex(key, ttl, json.dumps(state))
            return True
        except Exception as e:
            logger.error(f"Failed to set job state for {job_type}: {e}")
            return False
    
    async def get_job_state(self, job_type: str) -> Optional[dict]:
        """Get job state."""
        if not self.redis:
            return None
        
        key = f"job_state:{job_type}"
        try:
            state_str = await self.redis.get(key)
            if state_str:
                return json.loads(state_str)
            return None
        except Exception as e:
            logger.error(f"Failed to get job state for {job_type}: {e}")
            return None
    
    async def delete_job_state(self, job_type: str) -> bool:
        """Delete job state."""
        if not self.redis:
            return False
        
        key = f"job_state:{job_type}"
        try:
            result = await self.redis.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to delete job state for {job_type}: {e}")
            return False
    
    # Job progress tracking
    async def set_job_progress(self, job_id: int, progress: dict, ttl: int = 86400) -> bool:
        """Set job progress information."""
        if not self.redis:
            return False
        
        key = f"job_progress:{job_id}"
        try:
            await self.redis.setex(key, ttl, json.dumps(progress))
            return True
        except Exception as e:
            logger.error(f"Failed to set job progress for {job_id}: {e}")
            return False
    
    async def get_job_progress(self, job_id: int) -> Optional[dict]:
        """Get job progress information."""
        if not self.redis:
            return None
        
        key = f"job_progress:{job_id}"
        try:
            progress_str = await self.redis.get(key)
            if progress_str:
                return json.loads(progress_str)
            return None
        except Exception as e:
            logger.error(f"Failed to get job progress for {job_id}: {e}")
            return None
    
    # Utility methods
    async def health_check(self) -> bool:
        """Check if Redis is healthy."""
        try:
            if self.redis:
                await self.redis.ping()
                return True
            return False
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False


# Global Redis client instance
redis_client = RedisClient()


@asynccontextmanager
async def get_redis():
    if not redis_client._initialized:
        await redis_client.initialize()
    yield redis_client


async def acquire_movie_lock_safe(movie_id: int, timeout: int = 300) -> bool:
    """Safe wrapper for acquiring movie locks."""
    try:
        async with get_redis() as redis_conn:
            return await redis_conn.acquire_movie_lock(movie_id, timeout)
    except Exception as e:
        logger.error(f"Error acquiring movie lock for {movie_id}: {e}")
        return False


async def release_movie_lock_safe(movie_id: int) -> bool:
    """Safe wrapper for releasing movie locks."""
    try:
        async with get_redis() as redis_conn:
            return await redis_conn.release_movie_lock(movie_id)
    except Exception as e:
        logger.error(f"Error releasing movie lock for {movie_id}: {e}")
        return False