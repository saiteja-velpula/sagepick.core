import asyncio
import logging
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.services.tmdb_client.client import TMDBClient
from app.crud import movie, media_category, job_status,  job_log
from app.models.job_status import JobType
from app.utils.movie_processor import process_movie_batch

logger = logging.getLogger(__name__)

API_DELAY = 0.5  # 500ms delay between API calls
MOVIES_PER_CATEGORY = 20  # Number of movies to fetch per category


class CategoryRefreshJob:
    def __init__(self):
        self.job_type = JobType.CATEGORY_REFRESH
        self.tmdb_client = None
        
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
            "Hollywood": "get_hollywood_movies"
        }
    
    async def run(self):
        job_id = None
        
        async for db_session in get_session():
            try:
                # Get all media categories
                categories = await media_category.get_all_categories(db_session)
                
                if not categories:
                    logger.info("No media categories found. Skipping category refresh job.")
                    return
                
                # Filter categories that have matching methods
                valid_categories = [
                    cat for cat in categories 
                    if cat.name in self.category_methods
                ]
                
                if not valid_categories:
                    logger.info("No valid categories with matching TMDB methods found.")
                    return
                
                # Create job status record
                total_items = len(valid_categories) * MOVIES_PER_CATEGORY
                job_status_record = await job_status.create_job(
                    db_session, 
                    job_type=self.job_type,
                    total_items=total_items
                )
                job_id = job_status_record.id
                
                # Log job start
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Starting Category Refresh Job - updating {len(valid_categories)} categories with {MOVIES_PER_CATEGORY} movies each"
                )
                
                # Mark job as running
                await job_status.start_job(db_session, job_id)
                
                # Initialize Redis client
                await redis_client.initialize()
                
                # Initialize TMDB client
                self.tmdb_client = TMDBClient()
                
                # Process each category
                processed_count = await self._refresh_categories(db_session, job_id, valid_categories)
                
                # Update job progress
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Processed {processed_count} movies across all categories successfully"
                )
                
                # Mark job as completed
                await job_status.complete_job(
                    db_session, 
                    job_id, 
                    items_processed=processed_count
                )
                
                logger.info(f"Category Refresh Job completed successfully. Processed {processed_count} movies across {len(valid_categories)} categories.")
                break
                
            except Exception as e:
                logger.error(f"Category Refresh Job failed: {str(e)}", exc_info=True)
                
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
    
    async def _refresh_categories(self, db: AsyncSession, job_id: int, categories: List) -> int:
        """Refresh all media categories with latest movies."""
        total_processed = 0
        
        try:
            for category in categories:
                await job_log.log_info(
                    db,
                    job_id,
                    f"Updating category: {category.name}"
                )
                
                # Process this category
                category_processed = await self._refresh_single_category(db, job_id, category)
                total_processed += category_processed
                
                await job_log.log_info(
                    db,
                    job_id,
                    f"Updated category '{category.name}' with {category_processed} movies"
                )
                
                # Small delay between categories
                await asyncio.sleep(1.0)
            
            return total_processed
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error in _refresh_categories: {str(e)}"
            )
            raise
    
    async def _refresh_single_category(self, db: AsyncSession, job_id: int, category) -> int:
        """Refresh a single media category with latest movies."""
        try:
            processed_count = 0
            
            # Get the TMDB method for this category
            method_name = self.category_methods.get(category.name)
            if not method_name:
                await job_log.log_error(
                    db,
                    job_id,
                    f"No TMDB method found for category: {category.name}"
                )
                return 0
            
            # Get the method from TMDB client
            tmdb_method = getattr(self.tmdb_client, method_name, None)
            if not tmdb_method:
                await job_log.log_error(
                    db,
                    job_id,
                    f"TMDB method {method_name} not found for category: {category.name}"
                )
                return 0
            
            # Fetch movies from TMDB
            response = await tmdb_method(page=1)
            
            if not response or not response.movies:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"No movies found for category: {category.name}"
                )
                return 0
            
            movies = response.movies[:MOVIES_PER_CATEGORY]  # Limit to MOVIES_PER_CATEGORY
            
            # Extract movie IDs for processing
            movie_ids = []
            for movie_data in movies:
                movie_id = movie_data.id  # Use .id instead of .tmdb_id
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
            processed_count = 0
            if movie_ids:
                processed_count = await process_movie_batch(
                    db, self.tmdb_client, movie_ids, job_id
                )
                
                # Release locks for all processed movies
                for movie_id in movie_ids:
                    await redis_client.release_movie_lock(movie_id)
            
            # Update category with new movie IDs
            # First, get the actual movie IDs from our database (not TMDB IDs)
            db_movie_ids = []
            for tmdb_id in movie_ids:
                existing_movie = await movie.get_by_tmdb_id(db, tmdb_id)
                if existing_movie:
                    db_movie_ids.append(existing_movie.id)
            
            await media_category.update_category_movies(
                db,
                category_id=category.id,
                movie_ids=db_movie_ids
            )
            
            await job_log.log_info(
                db,
                job_id,
                f"Updated category '{category.name}' movie associations with {len(db_movie_ids)} movies"
            )
            
            return processed_count
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error refreshing category {category.name}: {str(e)}"
            )
            # Don't re-raise here, continue with other categories
            logger.error(f"Error refreshing category {category.name}: {str(e)}", exc_info=True)
            return 0


# Job instance for scheduler
category_refresh_job = CategoryRefreshJob()