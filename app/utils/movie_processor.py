import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud import job_log, job_status, movie
from app.models.movie import Movie, MovieCreate
from app.services.tmdb_client.client import TMDBClient
from app.utils.processors import genre_processor, keyword_processor
from app.utils.rate_limiter import rate_limited_call, tmdb_rate_limiter

logger = logging.getLogger(__name__)


@dataclass
class BatchProcessResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped_locked: int = 0


class MovieProcessor:
    def __init__(self):
        self.genre_processor = genre_processor
        self.keyword_processor = keyword_processor
        self.rate_limiter = tmdb_rate_limiter

    async def process_movie(
        self,
        db: AsyncSession,
        tmdb_client: TMDBClient,
        movie_id: int,
        job_id: int | None = None,
    ) -> Movie | None:
        """Process a single movie from TMDB.

        Args:
            db: Database session
            tmdb_client: TMDB API client
            movie_id: TMDB movie ID
            job_id: Optional job ID for logging

        Returns:
            Movie object if successful, None if failed
        """
        try:
            # Fetch movie data from TMDB with rate limiting
            movie_details, keywords = await asyncio.gather(
                rate_limited_call(
                    self.rate_limiter, lambda: tmdb_client.get_movie_by_id(movie_id)
                ),
                rate_limited_call(
                    self.rate_limiter, lambda: tmdb_client.get_movie_keywords(movie_id)
                ),
            )

            if not movie_details:
                if job_id:
                    await job_log.log_warning(
                        db, job_id, f"Could not fetch details for movie {movie_id}"
                    )
                return None

            # Process related entities
            genre_ids = await self.genre_processor.process_genres(
                db, movie_details.genres, job_id
            )
            keyword_ids = await self.keyword_processor.process_keywords(
                db, keywords, job_id
            )

            # Create movie object
            movie_create = MovieCreate(
                tmdb_id=movie_details.tmdb_id,
                title=movie_details.title,
                original_title=movie_details.original_title,
                overview=movie_details.overview or "",
                release_date=movie_details.release_date,
                runtime=movie_details.runtime,
                budget=movie_details.budget or 0,
                revenue=movie_details.revenue or 0,
                vote_average=movie_details.vote_average,
                vote_count=movie_details.vote_count,
                popularity=movie_details.popularity,
                poster_path=movie_details.poster_path,
                backdrop_path=movie_details.backdrop_path,
                adult=movie_details.adult,
                original_language=movie_details.original_language,
                status=movie_details.status or "",
            )

            # Save movie with relationships
            movie_obj = await movie.upsert_movie_with_relationships(
                db,
                movie_create=movie_create,
                genre_ids=genre_ids,
                keyword_ids=keyword_ids,
                commit=False,
            )

            await db.commit()
            return movie_obj

        except Exception as e:
            await db.rollback()

            if job_id:
                await job_log.log_error(
                    db, job_id, f"Error processing movie {movie_id}: {e!s}"
                )
            logger.error(f"Error processing movie {movie_id}: {e!s}", exc_info=True)
            return None


# Global processor instance
movie_processor = MovieProcessor()


async def process_movie_batch(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    movie_ids: list[int],
    job_id: int | None = None,
    *,
    use_locks: bool = False,
    cancel_event: asyncio.Event | None = None,
) -> BatchProcessResult:
    """Process multiple movies in batch.

    Args:
        db: Database session
        tmdb_client: TMDB API client
        movie_ids: List of TMDB movie IDs to process
        job_id: Optional job ID for logging
        use_locks: Whether to use Redis locks (requires redis_client)
        cancel_event: Event to signal cancellation

    Returns:
        BatchProcessResult with statistics
    """
    result = BatchProcessResult()

    # Import here to avoid circular dependencies
    from app.core.redis import redis_client

    # Deduplicate while preserving order
    unique_movie_ids = list(dict.fromkeys(movie_ids))

    for movie_id in unique_movie_ids:
        # Check for cancellation
        if cancel_event and cancel_event.is_set():
            if job_id:
                await job_log.log_warning(
                    db, job_id, "Cancellation requested; stopping movie processing"
                )
            break

        lock_acquired = True
        lock_error = False

        # Acquire lock if needed
        if use_locks:
            try:
                lock_acquired = await redis_client.acquire_movie_lock(movie_id)
            except Exception as e:
                logger.error(f"Failed to acquire lock for movie {movie_id}: {e}")
                lock_acquired = False
                lock_error = True

        if not lock_acquired:
            if lock_error:
                result.attempted += 1
                result.failed += 1
                if job_id:
                    await job_log.log_error(
                        db,
                        job_id,
                        f"Unable to obtain processing lock for movie {movie_id}",
                    )
            else:
                result.skipped_locked += 1
                if job_id:
                    await job_log.log_info(
                        db, job_id, f"Skipped movie {movie_id} due to existing lock"
                    )
            continue

        try:
            result.attempted += 1
            # Process the movie
            processed_movie = await movie_processor.process_movie(
                db, tmdb_client, movie_id, job_id
            )

            if processed_movie:
                result.succeeded += 1
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            if job_id:
                await job_log.log_error(
                    db, job_id, f"Unhandled error processing movie {movie_id}: {e!s}"
                )
            logger.error(
                f"Unhandled error processing movie {movie_id}: {e!s}", exc_info=True
            )

        finally:
            # Release lock if acquired
            if use_locks and lock_acquired:
                try:
                    await redis_client.release_movie_lock(movie_id)
                except Exception as e:
                    logger.error(f"Failed to release lock for movie {movie_id}: {e}")

    # Update job status if tracking
    if job_id and (result.succeeded or result.failed):
        await job_status.increment_counts(
            db, job_id, processed_delta=result.succeeded, failed_delta=result.failed
        )

    # Log summary
    if job_id:
        await job_log.log_info(
            db,
            job_id,
            f"Batch complete: {result.succeeded}/{result.attempted} succeeded, "
            f"{result.failed} failed"
            + (
                f" ({result.skipped_locked} skipped due to locks)"
                if result.skipped_locked
                else ""
            ),
        )

    return result
