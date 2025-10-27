import asyncio
import logging
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.core.job_execution import job_execution_manager
from app.core.settings import settings
from app.services.tmdb_client.client import TMDBClient
from app.crud import movie, media_category, job_status, job_log
from app.models.job_status import JobType
from app.utils.movie_processor import BatchProcessResult, process_movie_batch

logger = logging.getLogger(__name__)


class CategoryRefreshJob:
    def __init__(self):
        self.job_type = JobType.CATEGORY_REFRESH
        self.tmdb_client = None
        self.config = settings.JOBS

        # Mapping of category names to TMDB client methods
        self.category_methods = {
            "Trending": "get_trending_movies",
            "Popular": "get_popular_movies",
            "Top Rated": "get_top_rated_movies",
            "Upcoming": "get_upcoming_movies",
            "Now Playing": "get_now_playing_movies",
            "Bollywood": "get_bollywood_movies",
            "Tollywood": "get_tollywood_movies",
            "Kollywood": "get_kollywood_movies",
            "Mollywood": "get_mollywood_movies",
            "Sandalwood": "get_sandalwood_movies",
            "Hollywood": "get_hollywood_movies",
        }

    async def run(self):
        job_id = None
        cancel_event = None

        async for db_session in get_session():
            try:
                # Get all media categories
                categories = await media_category.get_all_categories(db_session)

                if not categories:
                    logger.info(
                        "No media categories found. Skipping category refresh job."
                    )
                    return

                # Filter categories that have matching methods
                valid_categories = [
                    cat for cat in categories if cat.name in self.category_methods
                ]

                if not valid_categories:
                    logger.info("No valid categories with matching TMDB methods found.")
                    return

                # Create job status record
                total_items = (
                    len(valid_categories) * self.config.movie_items_per_category
                )
                job_status_record = await job_status.create_job(
                    db_session, job_type=self.job_type, total_items=total_items
                )
                job_id = job_status_record.id
                cancel_event = await job_execution_manager.register(
                    job_id, self.job_type
                )

                # Log job start
                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Starting Category Refresh Job - updating "
                        f"{len(valid_categories)} categories with "
                        f"{self.config.movie_items_per_category} movies each"
                    ),
                )

                # Mark job as running
                await job_status.start_job(db_session, job_id)

                # Initialize Redis client
                await redis_client.initialize()

                # Initialize TMDB client
                self.tmdb_client = TMDBClient()

                # Process each category
                batch_result = await self._refresh_categories(
                    db_session, job_id, valid_categories, cancel_event
                )

                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Category refresh summary: "
                        f"{batch_result.succeeded} succeeded, "
                        f"{batch_result.failed} failed out of {batch_result.attempted} attempts"
                        + (
                            f" ({batch_result.skipped_locked} skipped due to locks)"
                            if batch_result.skipped_locked
                            else ""
                        )
                    ),
                )

                failure_rate = (
                    batch_result.failed / batch_result.attempted
                    if batch_result.attempted > 0
                    else 0
                )

                if (
                    batch_result.attempted > 0
                    and failure_rate >= self.config.error_rate_threshold
                ):
                    await job_log.log_error(
                        db_session,
                        job_id,
                        (
                            "Category refresh encountered a high failure rate "
                            f"({failure_rate:.0%}); marking job as failed"
                        ),
                    )
                    await job_status.fail_job(
                        db_session,
                        job_id,
                        processed_items=batch_result.succeeded,
                        failed_items=batch_result.failed,
                    )
                    logger.error(
                        "Category Refresh Job failed due to error rate %.0f%%",
                        failure_rate * 100,
                    )
                else:
                    await job_status.complete_job(
                        db_session,
                        job_id,
                        items_processed=batch_result.succeeded,
                        failed_items=batch_result.failed,
                    )

                    logger.info(
                        "Category Refresh Job completed successfully. Processed %d movies across %d categories.",
                        batch_result.succeeded,
                        len(valid_categories),
                    )

                break
            except asyncio.CancelledError:
                logger.warning("Category Refresh Job cancellation requested")
                if job_id:
                    await job_log.log_warning(
                        db_session,
                        job_id,
                        "Cancellation requested; aborting category refresh run",
                    )
                    await job_status.cancel_job(db_session, job_id)
                return
            except Exception as e:
                logger.error(f"Category Refresh Job failed: {str(e)}", exc_info=True)

                if job_id:
                    await job_log.log_error(
                        db_session, job_id, f"Job failed with error: {str(e)}"
                    )
                    await job_status.fail_job(db_session, job_id)

                raise
            finally:
                if job_id is not None:
                    await job_execution_manager.unregister(job_id)
                if self.tmdb_client:
                    await self.tmdb_client.close()

    async def _refresh_categories(
        self,
        db: AsyncSession,
        job_id: int,
        categories: List,
        cancel_event: Optional[asyncio.Event],
    ) -> BatchProcessResult:
        """Refresh all media categories with latest movies."""
        total_attempted = 0
        total_succeeded = 0
        total_failed = 0
        total_skipped_locked = 0

        try:
            for category in categories:
                if cancel_event and cancel_event.is_set():
                    await job_log.log_warning(
                        db,
                        job_id,
                        "Cancellation requested; stopping remaining category updates",
                    )
                    break

                await job_log.log_info(
                    db, job_id, f"Updating category: {category.name}"
                )

                # Process this category
                category_result = await self._refresh_single_category(
                    db, job_id, category, cancel_event
                )
                total_attempted += category_result.attempted
                total_succeeded += category_result.succeeded
                total_failed += category_result.failed
                total_skipped_locked += category_result.skipped_locked

                await job_log.log_info(
                    db,
                    job_id,
                    f"Updated category '{category.name}' with {category_result.succeeded} movies",
                )

                if cancel_event and cancel_event.is_set():
                    break

                await asyncio.sleep(0.1)

            return BatchProcessResult(
                attempted=total_attempted,
                succeeded=total_succeeded,
                failed=total_failed,
                skipped_locked=total_skipped_locked,
            )

        except Exception as e:
            await job_log.log_error(
                db, job_id, f"Error in _refresh_categories: {str(e)}"
            )
            raise

    async def _refresh_single_category(
        self,
        db: AsyncSession,
        job_id: int,
        category,
        cancel_event: Optional[asyncio.Event],
    ) -> BatchProcessResult:
        """Refresh a single media category with latest movies."""
        try:
            category_result = BatchProcessResult()

            # Get the TMDB method for this category
            method_name = self.category_methods.get(category.name)
            if not method_name:
                await job_log.log_error(
                    db, job_id, f"No TMDB method found for category: {category.name}"
                )
                return category_result

            # Get the method from TMDB client
            tmdb_method = getattr(self.tmdb_client, method_name, None)
            if not tmdb_method:
                await job_log.log_error(
                    db,
                    job_id,
                    f"TMDB method {method_name} not found for category: {category.name}",
                )
                return category_result

            # Fetch movies from TMDB
            response = await tmdb_method(page=1)

            if not response or not response.movies:
                await job_log.log_warning(
                    db, job_id, f"No movies found for category: {category.name}"
                )
                return category_result

            movies = response.movies[: self.config.movie_items_per_category]

            # Extract movie IDs for processing
            movie_ids = [
                movie_data.tmdb_id for movie_data in movies if movie_data.tmdb_id
            ]

            # Process movies in batch using utility function
            if movie_ids:
                category_result = await process_movie_batch(
                    db,
                    self.tmdb_client,
                    movie_ids,
                    job_id,
                    use_locks=True,
                    cancel_event=cancel_event,
                )

            if category_result.skipped_locked:
                await job_log.log_info(
                    db,
                    job_id,
                    f"Skipped {category_result.skipped_locked} movies for category '{category.name}' due to existing locks",
                )

            # Update category with new movie IDs
            # First, get the actual movie IDs from our database (not TMDB IDs)
            db_movie_ids = []
            for tmdb_id in movie_ids:
                existing_movie = await movie.get_by_tmdb_id(db, tmdb_id)
                if existing_movie:
                    db_movie_ids.append(existing_movie.id)

            await media_category.update_category_movies(
                db, category_id=category.id, movie_ids=db_movie_ids
            )

            await job_log.log_info(
                db,
                job_id,
                f"Updated category '{category.name}' movie associations with {len(db_movie_ids)} movies",
            )

            return category_result

        except Exception as e:
            await job_log.log_error(
                db, job_id, f"Error refreshing category {category.name}: {str(e)}"
            )
            # Don't re-raise here, continue with other categories
            logger.error(
                f"Error refreshing category {category.name}: {str(e)}", exc_info=True
            )
            return category_result


# Job instance for scheduler
category_refresh_job = CategoryRefreshJob()
