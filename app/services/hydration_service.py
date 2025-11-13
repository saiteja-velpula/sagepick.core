"""Simple background hydration worker service.

This service runs a background worker that:
1. Polls Redis queue for movie TMDB IDs
2. Uses fetch_and_insert_full() processor to hydrate movies
3. Runs independently without blocking endpoints
"""

import asyncio
import contextlib
import logging

from app.core.db import get_session
from app.core.redis import redis_client
from app.core.tmdb import get_tmdb_client
from app.utils.movie_processor import fetch_and_insert_full

logger = logging.getLogger(__name__)


class HydrationService:
    """Simple background hydration worker."""

    QUEUE_KEY = "hydration:queue"
    BATCH_SIZE = 10  # Process this many movies at once
    POLL_INTERVAL = 1  # seconds between queue checks

    def __init__(self):
        self._running = False
        self._worker_task: asyncio.Task | None = None

    def queue_movies_batch_background(self, tmdb_ids: list[int]) -> None:
        """Queue movies for hydration in background (fire-and-forget).

        This creates a background task that doesn't block the caller.
        Use this in API endpoints to queue movies without waiting.

        Args:
            tmdb_ids: List of TMDB movie IDs to queue
        """
        if not tmdb_ids:
            return

        async def _queue_task():
            try:
                # Add all IDs to Redis set (no duplicates)
                if tmdb_ids:
                    await redis_client.sadd(self.QUEUE_KEY, *tmdb_ids)
                    logger.info(
                        f"Queued {len(tmdb_ids)} movies for background hydration"
                    )
            except Exception as e:
                logger.error(f"Error queuing movies: {e}", exc_info=True)

        # Create task without awaiting (fire-and-forget)
        task = asyncio.create_task(_queue_task())
        # Add task to background set to prevent garbage collection
        task.add_done_callback(lambda _: None)
        logger.debug(f"Created background task to queue {len(tmdb_ids)} movies")

    async def get_queue_size(self) -> int:
        """Get current size of hydration queue."""
        try:
            return await redis_client.scard(self.QUEUE_KEY)
        except Exception as e:
            logger.error(f"Failed to get queue size: {e}")
            return 0

    async def _worker_loop(self):
        """Main worker loop that processes the hydration queue."""
        logger.info("Hydration worker started")

        while self._running:
            try:
                queue_size = await self.get_queue_size()

                if queue_size == 0:
                    # No work to do, sleep and check again
                    await asyncio.sleep(self.POLL_INTERVAL)
                    continue

                logger.info(f"Processing hydration queue (size: {queue_size})")

                # Pop movies from queue (batch)
                tmdb_ids = []
                for _ in range(min(self.BATCH_SIZE, queue_size)):
                    tmdb_id = await redis_client.spop(self.QUEUE_KEY)
                    if tmdb_id:
                        try:
                            tmdb_ids.append(int(tmdb_id))
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid tmdb_id in queue: {tmdb_id}")

                if not tmdb_ids:
                    await asyncio.sleep(self.POLL_INTERVAL)
                    continue

                # Process batch using Processor 2
                tmdb_client = await get_tmdb_client()

                async for db_session in get_session():
                    succeeded = 0
                    failed = 0

                    for tmdb_id in tmdb_ids:
                        if not self._running:
                            # If shutting down, put unprocessed movies back
                            remaining = tmdb_ids[tmdb_ids.index(tmdb_id) :]
                            if remaining:
                                await redis_client.sadd(self.QUEUE_KEY, *remaining)
                            logger.info(
                                "Hydration worker shutting down, "
                                "queued remaining movies"
                            )
                            return

                        try:
                            result = await fetch_and_insert_full(
                                db=db_session,
                                tmdb_client=tmdb_client,
                                tmdb_id=tmdb_id,
                                hydration_source="background",
                                job_id=None,
                            )

                            if result:
                                succeeded += 1
                                logger.debug(f"Successfully hydrated movie {tmdb_id}")
                            else:
                                failed += 1
                                logger.warning(f"Failed to hydrate movie {tmdb_id}")

                        except Exception as e:
                            failed += 1
                            logger.error(
                                f"Error hydrating movie {tmdb_id}: {e}", exc_info=True
                            )

                    logger.info(
                        f"Batch complete: {succeeded} succeeded, {failed} failed "
                        f"out of {len(tmdb_ids)} movies"
                    )
                    break  # Exit the async generator

            except asyncio.CancelledError:
                logger.info("Hydration worker received cancellation")
                break
            except Exception as e:
                logger.error(f"Error in hydration worker loop: {e}", exc_info=True)
                await asyncio.sleep(5)  # Wait before retrying

        logger.info("Hydration worker stopped")

    async def start_worker(self):
        """Start the background hydration worker."""
        if self._running:
            logger.warning("Hydration worker already running")
            return

        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("Hydration worker task created and started")

    async def stop_worker(self):
        """Stop the background hydration worker gracefully."""
        if not self._running:
            return

        logger.info("Stopping hydration worker...")
        self._running = False

        if self._worker_task:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task

        logger.info("Hydration worker stopped")


# Global service instance
hydration_service = HydrationService()
