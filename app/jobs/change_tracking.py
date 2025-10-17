import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.services.tmdb_client.client import TMDBClient
from app.crud.movie import movie
from app.crud.genre import genre
from app.crud.keyword import keyword
from app.crud.job_status import job_status
from app.crud.job_log import job_log
from app.models.job_status import JobType, JobExecutionStatus
from app.models.movie import MovieCreate

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
                    
                    try:
                        await self._process_changed_movie(db, job_id, movie_id)
                        processed_count += 1
                        
                        # Add delay between API calls
                        await asyncio.sleep(API_DELAY)
                        
                    finally:
                        # Always release the lock
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
    
    async def _process_changed_movie(self, db: AsyncSession, job_id: int, movie_id: int):
        """Process a changed movie: fetch latest details, keywords, and update database."""
        try:
            await job_log.log_info(
                db,
                job_id,
                f"Processing changed movie {movie_id}"
            )
            
            # Fetch movie details
            movie_details = await self.tmdb_client.get_movie_by_id(movie_id)
            if not movie_details:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"Could not fetch details for changed movie {movie_id}"
                )
                return
            
            # Fetch movie keywords
            keywords_response = await self.tmdb_client.get_movie_keywords(movie_id)
            
            # Add delay after API calls
            await asyncio.sleep(API_DELAY)
            
            # Process genres
            genre_ids = []
            if movie_details.genres:
                for genre_data in movie_details.genres:
                    genre_obj = await genre.upsert_genre(
                        db,
                        genre_id=genre_data.id,
                        name=genre_data.name
                    )
                    genre_ids.append(genre_obj.id)
            
            # Process keywords
            keyword_db_ids = []
            for kw in keywords_response.keywords:
                keyword_obj = await keyword.upsert_keyword(
                    db,
                    keyword_id=kw.id,
                    name=kw.name
                )
                keyword_db_ids.append(keyword_obj.id)
            
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
                status=movie_details.status or ""
            )
            
            # Upsert movie with relationships (this will update existing or create new)
            movie_obj = await movie.upsert_movie_with_relationships(
                db,
                movie_create=movie_create,
                genre_ids=genre_ids,
                keyword_ids=keyword_db_ids
            )
            
            await job_log.log_info(
                db,
                job_id,
                f"Successfully updated changed movie: {movie_obj.title} (ID: {movie_obj.tmdb_id})"
            )
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error processing changed movie {movie_id}: {str(e)}"
            )
            # Don't re-raise here, continue with other movies
            logger.error(f"Error processing changed movie {movie_id}: {str(e)}", exc_info=True)


# Job instance for scheduler
change_tracking_job = ChangeTrackingJob()