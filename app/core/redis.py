import logging

import redis.asyncio as redis

from app.core.settings import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis client wrapper for async operations."""

    def __init__(self):
        self.redis: redis.Redis | None = None
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
                    retry_on_timeout=True,
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

    # Common Methods
    async def get(self, key: str) -> str | None:
        """Get a value from Redis."""
        if not self.redis:
            return None
        try:
            return await self.redis.get(key)
        except Exception as exc:
            logger.error(f"Failed to get Redis key {key}: {exc}")
            return None

    async def setex(self, key: str, ttl: int, value: str) -> None:
        """Set a key with expiration time."""
        if not self.redis:
            return
        try:
            await self.redis.setex(key, ttl, value)
        except Exception as exc:
            logger.error(f"Failed to setex Redis key {key}: {exc}")

    async def keys(self, pattern: str) -> list[str]:
        """Get keys matching a pattern."""
        if not self.redis:
            return []
        try:
            return await self.redis.keys(pattern)
        except Exception as exc:
            logger.error(f"Failed to get Redis keys for pattern {pattern}: {exc}")
            return []

    async def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        if not self.redis or not keys:
            return 0
        try:
            return await self.redis.delete(*keys)
        except Exception as exc:
            logger.error(f"Failed to delete Redis keys {keys}: {exc}")
            return 0

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

    # Movie ID Locking for conflict resolution
    async def acquire_movie_lock(self, movie_id: int, timeout: int = 300) -> bool:
        """Acquire lock for a movie ID to prevent concurrent processing.
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

    # Redis Set operations for hydration queue
    async def sadd(self, key: str, *values) -> int:
        """Add one or more members to a set."""
        if not self.redis:
            return 0
        try:
            return await self.redis.sadd(key, *values)
        except Exception as exc:
            logger.error(f"Failed to sadd to Redis set {key}: {exc}")
            return 0

    async def sismember(self, key: str, value) -> bool:
        """Check if a value is a member of a set."""
        if not self.redis:
            return False
        try:
            return await self.redis.sismember(key, value)
        except Exception as exc:
            logger.error(f"Failed to check sismember in Redis set {key}: {exc}")
            return False

    async def scard(self, key: str) -> int:
        """Get the number of members in a set."""
        if not self.redis:
            return 0
        try:
            return await self.redis.scard(key)
        except Exception as exc:
            logger.error(f"Failed to get scard for Redis set {key}: {exc}")
            return 0

    async def spop(self, key: str) -> str | None:
        """Remove and return a random member from a set."""
        if not self.redis:
            return None
        try:
            return await self.redis.spop(key)
        except Exception as exc:
            logger.error(f"Failed to spop from Redis set {key}: {exc}")
            return None

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        if not self.redis:
            return False
        try:
            result = await self.redis.exists(key)
            return result > 0
        except Exception as exc:
            logger.error(f"Failed to check existence of Redis key {key}: {exc}")
            return False


# Global Redis client instance
redis_client = RedisClient()
