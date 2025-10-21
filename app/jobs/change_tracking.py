import logging

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.services.tmdb_client.client import TMDBClient
from app.crud import job_status, job_log
from app.models.job_status import JobType
from app.utils.movie_processor import process_movie_batch

logger = logging.getLogger(__name__)

API_DELAY = 0.5  # 500ms delay between API calls
ITEMS_PER_PAGE = 100  # TMDB returns up to 100 items per page for changes


class ChangeTrackingJob:
    def __init__(self):
        self.job_type = JobType.CHANGE_TRACKING
        self.tmdb_client = None
    
    async def run(self):
        """Main job execution method."""
        job_id = None
        
        async for db_session in get_session():
            try:
                # Create job status record (we don't know total items yet)
                job_status_record = await job_status.create_job(
                    db_session, 
                    job_type=self.job_type,
                    total_items=0
                )
                job_id = job_status_record.id
                
                # Log job start
                await job_log.log_info(
                    db_session,
                    job_id,
                    "Starting Change Tracking Job - fetching all changed movies from last 24 hours"
                )
                
                # Mark job as running
                await job_status.start_job(db_session, job_id)
                
                # Initialize Redis client
                await redis_client.initialize()
                
                # Initialize TMDB client
                self.tmdb_client = TMDBClient()
                
                # Track changes and process movies
                processed_count = await self._track_changes(db_session, job_id)
                
                # Update job progress
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Processed {processed_count} changed movies successfully"
                )
                
                # Mark job as completed
                await job_status.complete_job(
                    db_session, 
                    job_id, 
                    items_processed=processed_count
                )
                
                logger.info(f"Change Tracking Job completed successfully. Processed {processed_count} changed movies.")
                break
                
            except Exception as e:
                logger.error(f"Change Tracking Job failed: {str(e)}", exc_info=True)
                
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
    
    async def _track_changes(self, db: AsyncSession, job_id: int) -> int:
        """Track changes from TMDB changes endpoint."""
        try:
            processed_count = 0
            current_page = 1
            total_pages = 1
            
            while current_page <= total_pages:
                await job_log.log_info(
                    db,
                    job_id,
                    f"Fetching changes page {current_page}/{total_pages}"
                )
                
                # Fetch changes from TMDB
                changes_response = await self.tmdb_client.get_movie_changes(page=current_page)

                if not changes_response or not changes_response.results:
                    await job_log.log_warning(
                        db,
                        job_id,
                        f"No changes found on page {current_page}"
                    )
                    break
                
                # Update total pages info
                total_pages = changes_response.total_pages if changes_response.total_pages else 1
                changed_movies = changes_response.results
                await job_log.log_info(
                    db,
                    job_id,
                    f"Found {len(changed_movies)} changed movies on page {current_page}/{total_pages}"
                )
                
                # Update total items estimate if this is the first page
                if current_page == 1:
                    estimated_total = total_pages * ITEMS_PER_PAGE
                    await job_status.update_total_items(db, job_id, estimated_total)
                
                # Process each changed movie
                movie_ids = []
                for movie_data in changed_movies:
                    movie_id = movie_data.id
                    if not movie_id:
                        continue
                    
                    # Check Redis lock for this movie ID
                    lock_acquired = await redis_client.acquire_movie_lock(movie_id)
                    if not lock_acquired:
                        await job_log.log_info(
                            db,
                            job_id,
                            f"Skipping movie {movie_id} - locked by another job"
                        )
                        continue
                    
                    movie_ids.append(movie_id)
                
                # Process movies in batch using utility function
                if movie_ids:
                    batch_processed = await process_movie_batch(
                        db, self.tmdb_client, movie_ids, job_id
                    )
                    processed_count += batch_processed
                    
                    # Release locks for all processed movies
                    for movie_id in movie_ids:
                        await redis_client.release_movie_lock(movie_id)
                
                current_page += 1
                
                # Log progress every 10 pages
                if current_page % 10 == 0:
                    await job_log.log_info(
                        db,
                        job_id,
                        f"Progress: Processed {processed_count} movies, on page {current_page}/{total_pages}"
                    )
            
            return processed_count
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error in _track_changes: {str(e)}"
            )
            raise


# Job instance for scheduler
change_tracking_job = ChangeTrackingJob()