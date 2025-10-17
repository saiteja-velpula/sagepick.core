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
from app.crud.media_category import media_category
from app.crud.job_status import job_status
from app.crud.job_log import job_log
from app.models.job_status import JobType, JobExecutionStatus
from app.models.movie import MovieCreate

logger = logging.getLogger(__name__)

API_DELAY = 0.5  # 500ms delay between API calls
MOVIES_PER_CATEGORY = 20  # Number of movies to fetch per category


class CategoryRefreshJob:
    def __init__(self):
        self.job_type = JobType.CATEGORY_REFRESH
        self.tmdb_client = None
        
        # Mapping of category names to TMDB client methods
        self.category_methods = {
            "trending": "get_trending_movies",
            "popular": "get_popular_movies", 
            "top_rated": "get_top_rated_movies",
            "upcoming": "get_upcoming_movies",
            "now_playing": "get_now_playing_movies",
            "bollywood": "get_bollywood_movies",
            "tollywood": "get_tollywood_movies",
            "kollywood": "get_kollywood_movies",
            "mollywood": "get_mollywood_movies",
            "sandalwood": "get_sandalwood_movies",
            "hollywood": "get_hollywood_movies"
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
                    if cat.name.lower() in self.category_methods
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
            method_name = self.category_methods.get(category.name.lower())
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
            
            # Extract movie IDs for updating category
            new_movie_ids = []
            
            # Process each movie
            for movie_data in movies:
                movie_id = movie_data.tmdb_id
                if not movie_id:
                    continue
                
                new_movie_ids.append(movie_id)
                
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
                    await self._process_category_movie(db, job_id, movie_id, category.name)
                    processed_count += 1
                    
                    # Add delay between API calls
                    await asyncio.sleep(API_DELAY)
                    
                finally:
                    # Always release the lock
                    await redis_client.release_movie_lock(movie_id)
            
            # Update category with new movie IDs
            # First, get the actual movie IDs from our database (not TMDB IDs)
            db_movie_ids = []
            for tmdb_id in new_movie_ids:
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
    
    async def _process_category_movie(self, db: AsyncSession, job_id: int, movie_id: int, category_name: str):
        """Process a movie from category: fetch details, keywords, and upsert to database."""
        try:
            await job_log.log_info(
                db,
                job_id,
                f"Processing {category_name} movie {movie_id}"
            )
            
            # Fetch movie details
            movie_details = await self.tmdb_client.get_movie_by_id(movie_id)
            if not movie_details:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"Could not fetch details for {category_name} movie {movie_id}"
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
                status=movie_details.status or "",
            )
            
            # Upsert movie with relationships
            movie_obj = await movie.upsert_movie_with_relationships(
                db,
                movie_create=movie_create,
                genre_ids=genre_ids,
                keyword_ids=keyword_db_ids
            )
            
            await job_log.log_info(
                db,
                job_id,
                f"Successfully processed {category_name} movie: {movie_obj.title} (ID: {movie_obj.tmdb_id})"
            )
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error processing {category_name} movie {movie_id}: {str(e)}"
            )
            # Don't re-raise here, continue with other movies
            logger.error(f"Error processing {category_name} movie {movie_id}: {str(e)}", exc_info=True)


# Job instance for scheduler
category_refresh_job = CategoryRefreshJob()