import logging

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.services.tmdb_client.client import TMDBClient
from app.services.tmdb_client.models import MovieSearchParams
from app.crud import job_status, job_log
from app.models import JobType
from app.utils.movie_processor import process_movie_batch

logger = logging.getLogger(__name__)

ITEMS_PER_RUN = 20
API_DELAY = 0.5  # 500ms delay between API calls


class MovieDiscoveryJob:
    def __init__(self):
        self.job_type = JobType.MOVIE_DISCOVERY
        self.tmdb_client = None
        self.current_page = 1
    
    async def run(self):
        """Main job execution method."""
        job_id = None
        
        async for db_session in get_session():
            try:
                # Create job status record
                job_status_record = await job_status.create_job(
                    db_session, 
                    job_type=self.job_type,
                    total_items=ITEMS_PER_RUN
                )
                job_id = job_status_record.id
                
                # Log job start
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Starting Movie Discovery Job - fetching {ITEMS_PER_RUN} movies"
                )
                
                # Mark job as running
                await job_status.start_job(db_session, job_id)
                
                # Initialize Redis client
                await redis_client.initialize()
                
                # Get current page from Redis or start from 1
                job_state = await redis_client.get_job_state(self.job_type.value)
                if job_state and "current_page" in job_state:
                    self.current_page = job_state["current_page"]
                
                # Initialize TMDB client
                self.tmdb_client = TMDBClient()
                
                # Fetch and process movies
                processed_count = await self._discover_movies(db_session, job_id)
                
                # Update job progress
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Processed {processed_count} movies successfully"
                )
                
                # Mark job as completed
                await job_status.complete_job(
                    db_session, 
                    job_id, 
                    items_processed=processed_count
                )
                
                # Update current page in Redis for next run
                self.current_page += 1
                await redis_client.set_job_state(
                    self.job_type.value, 
                    {"current_page": self.current_page}
                )
                
                logger.info(f"Movie Discovery Job completed successfully. Processed {processed_count} movies.")
                break
                
            except Exception as e:
                logger.error(f"Movie Discovery Job failed: {str(e)}", exc_info=True)
                
                if job_id:
                    await job_log.log_error(
                        db_session,
                        job_id,
                        f"Job failed with error: {str(e)}"
                    )
                    await job_status.fail_job(db_session, job_id, str(e))
                
                raise
            finally:
                if self.tmdb_client:
                    await self.tmdb_client.close()
    
    async def _discover_movies(self, db: AsyncSession, job_id: int) -> int:
        """Discover movies from TMDB discover endpoint."""
        try:
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
                return 0
            
            movies = discover_response.movies
            total_pages = discover_response.pagination.total_pages
            
            await job_log.log_info(
                db,
                job_id,
                f"Found {len(movies)} movies on page {self.current_page}/{total_pages}"
            )
            
            # Process movies (limit to ITEMS_PER_RUN)
            movie_ids = []
            skipped_locked = 0
            for movie_data in movies[:ITEMS_PER_RUN]:
                movie_id = movie_data.tmdb_id
                if not movie_id:
                    continue
                
                # Check Redis lock for this movie ID
                lock_acquired = await redis_client.acquire_movie_lock(movie_id)
                if not lock_acquired:
                    skipped_locked += 1
                    continue
                
                movie_ids.append(movie_id)
            
            # Process movies in batch using utility function
            processed_count = 0
            if movie_ids:
                processed_count = await process_movie_batch(
                    db, self.tmdb_client, movie_ids, job_id
                )
                
                # Release locks for all processed movies
                for movie_id in movie_ids:
                    await redis_client.release_movie_lock(movie_id)

            if skipped_locked:
                await job_log.log_info(
                    db,
                    job_id,
                    f"Skipped {skipped_locked} movies on page {self.current_page} due to existing locks"
                )
            
            # Reset to page 1 if we've reached the end
            if self.current_page >= total_pages:
                self.current_page = 0  # Will be incremented to 1 after job completion
                await job_log.log_info(
                    db,
                    job_id,
                    "Reached end of discover pages, resetting to page 1"
                )
            
            return processed_count
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error in _discover_movies: {str(e)}"
            )
            raise


# Job instance for scheduler
movie_discovery_job = MovieDiscoveryJob()