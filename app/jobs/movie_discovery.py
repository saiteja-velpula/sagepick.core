import asyncio
import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.core.job_execution import job_execution_manager
from app.core.settings import settings
from app.services.tmdb_client.client import TMDBClient
from app.services.tmdb_client.models import MovieSearchParams
from app.crud import job_status, job_log, movie_discovery_state
from app.models import JobType
from app.utils.movie_processor import BatchProcessResult, process_movie_batch

logger = logging.getLogger(__name__)


class MovieDiscoveryJob:
    def __init__(self):
        self.job_type = JobType.MOVIE_DISCOVERY
        self.tmdb_client = None
        self.current_page = 1
        self.config = settings.JOBS
    
    async def run(self):
        """Main job execution method."""
        job_id = None
        cancel_event = None
        
        async for db_session in get_session():
            try:
                # Create job status record
                job_status_record = await job_status.create_job(
                    db_session, 
                    job_type=self.job_type,
                    total_items=self.config.movie_items_per_run
                )
                job_id = job_status_record.id
                cancel_event = await job_execution_manager.register(job_id, self.job_type)
                
                # Log job start
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Starting Movie Discovery Job - fetching {self.config.movie_items_per_run} movies"
                )
                
                # Mark job as running
                await job_status.start_job(db_session, job_id)
                
                # Initialize Redis client
                await redis_client.initialize()
                
                # Load last persisted page from the database
                self.current_page = await movie_discovery_state.get_current_page(db_session)
                
                # Initialize TMDB client
                self.tmdb_client = TMDBClient()
                
                # Fetch and process movies
                batch_result = await self._discover_movies(db_session, job_id, cancel_event)

                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Movie discovery summary: "
                        f"{batch_result.succeeded} succeeded, "
                        f"{batch_result.failed} failed out of {batch_result.attempted} attempts"
                        + (
                            f" ({batch_result.skipped_locked} skipped due to locks)"
                            if batch_result.skipped_locked
                            else ""
                        )
                    )
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
                            "Movie discovery encountered a high failure rate "
                            f"({failure_rate:.0%}); marking job as failed"
                        )
                    )
                    await job_status.fail_job(
                        db_session,
                        job_id,
                        processed_items=batch_result.succeeded,
                        failed_items=batch_result.failed
                    )
                    logger.error(
                        "Movie Discovery Job failed due to error rate %.0f%%",
                        failure_rate * 100
                    )
                else:
                    await job_status.complete_job(
                        db_session,
                        job_id,
                        items_processed=batch_result.succeeded,
                        failed_items=batch_result.failed
                    )

                    # Persist the next page for the upcoming run
                    self.current_page += 1
                    await movie_discovery_state.update_current_page(db_session, self.current_page)

                    logger.info(
                        "Movie Discovery Job completed successfully. Processed %d movies.",
                        batch_result.succeeded
                    )

                break
            except asyncio.CancelledError:
                logger.warning("Movie Discovery Job cancellation requested")
                if job_id:
                    await job_log.log_warning(
                        db_session,
                        job_id,
                        "Cancellation requested; aborting movie discovery run"
                    )
                    await job_status.cancel_job(
                        db_session,
                        job_id,
                        processed_items=None,
                        failed_items=None
                    )
                return
            except Exception as e:
                logger.error(f"Movie Discovery Job failed: {str(e)}", exc_info=True)
                
                if job_id:
                    await job_log.log_error(
                        db_session,
                        job_id,
                        f"Job failed with error: {str(e)}"
                    )
                    await job_status.fail_job(db_session, job_id)
                
                raise
            finally:
                if job_id is not None:
                    await job_execution_manager.unregister(job_id)
                if self.tmdb_client:
                    await self.tmdb_client.close()
    
    async def _discover_movies(self, db: AsyncSession, job_id: int, cancel_event: Optional[asyncio.Event]) -> BatchProcessResult:
        """Discover movies from TMDB discover endpoint."""
        try:
            if cancel_event and cancel_event.is_set():
                await job_log.log_warning(
                    db,
                    job_id,
                    "Cancellation requested before movie discovery page fetch"
                )
                return BatchProcessResult()
            
            # Fetch discover page
            await job_log.log_info(
                db,
                job_id,
                f"Fetching discover page {self.current_page}"
            )
            
            # Create search params for discovery
            search_params = MovieSearchParams(
                page=self.current_page,
                sort_by="title.asc"
            )
            
            discover_response = await self.tmdb_client.discover_movies(search_params)
            
            if not discover_response or not discover_response.movies:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"No movies found on page {self.current_page}"
                )
                return BatchProcessResult()
            
            movies = discover_response.movies
            total_pages = discover_response.pagination.total_pages
            
            await job_log.log_info(
                db,
                job_id,
                f"Found {len(movies)} movies on page {self.current_page}/{total_pages}"
            )
            
            # Process movies (limit to configured batch size)
            movie_ids = [
                movie_data.tmdb_id
                for movie_data in movies[: self.config.movie_items_per_run]
                if movie_data.tmdb_id
            ]

            batch_result = BatchProcessResult()
            if movie_ids:
                batch_result = await process_movie_batch(
                    db,
                    self.tmdb_client,
                    movie_ids,
                    job_id,
                    use_locks=True,
                    cancel_event=cancel_event
                )

            if batch_result.skipped_locked:
                await job_log.log_info(
                    db,
                    job_id,
                    f"Skipped {batch_result.skipped_locked} movies on page {self.current_page} due to existing locks"
                )
            
            # Reset to page 1 if we've reached the end
            if self.current_page >= total_pages:
                self.current_page = 0  # Will be incremented to 1 after job completion
                await job_log.log_info(
                    db,
                    job_id,
                    "Reached end of discover pages, resetting to page 1"
                )
            
            return batch_result
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error in _discover_movies: {str(e)}"
            )
            raise


# Job instance for scheduler
movie_discovery_job = MovieDiscoveryJob()