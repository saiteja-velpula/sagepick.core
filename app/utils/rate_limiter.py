import asyncio
import logging
from time import monotonic
from typing import Awaitable, Callable, TypeVar

from app.core.settings import settings

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AsyncRateLimiter:
    def __init__(self, max_per_second: int):
        self._interval = 1.0 / max(1, max_per_second)
        self._lock = asyncio.Lock()
        self._last_acquire = 0.0

    async def acquire(self) -> None:
        """Acquire rate limit token, sleeping if necessary."""
        async with self._lock:
            now = monotonic()
            elapsed = now - self._last_acquire
            if elapsed < self._interval:
                sleep_time = self._interval - elapsed
                await asyncio.sleep(sleep_time)
            self._last_acquire = monotonic()


async def rate_limited_call(
    limiter: AsyncRateLimiter, coro_factory: Callable[[], Awaitable[T]]
) -> T:
    """Execute a coroutine with rate limiting."""
    await limiter.acquire()
    return await coro_factory()


# Global rate limiter
tmdb_rate_limiter = AsyncRateLimiter(settings.TMDB_MAX_REQUESTS_PER_SECOND)
