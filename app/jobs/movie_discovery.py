import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_session
from app.core.redis import redis_client
from app.services.tmdb_client.client import TMDBClient
from app.services.tmdb_client.models import MovieSearchParams
from app.crud import movie, genre, keyword, job_status, job_log
from app.models import JobType, MovieCreate

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
            processed_count = 0
            for movie_data in movies[:ITEMS_PER_RUN]:
                movie_id = movie_data.tmdb_id
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
                    await self._process_single_movie(db, job_id, movie_id)
                    processed_count += 1
                    
                    # Add delay between API calls
                    await asyncio.sleep(API_DELAY)
                    
                finally:
                    # Always release the lock
                    await redis_client.release_movie_lock(movie_id)
            
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
    
    async def _process_single_movie(self, db: AsyncSession, job_id: int, movie_id: int):
        """Process a single movie: fetch details, keywords, and upsert to database."""
        try:
            await job_log.log_info(
                db,
                job_id,
                f"Processing movie {movie_id}"
            )
            
            # Fetch movie details
            movie_details = await self.tmdb_client.get_movie_by_id(movie_id)
            if not movie_details:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"Could not fetch details for movie {movie_id}"
                )
                return
            
            # Fetch movie keywords
            keywords = await self.tmdb_client.get_movie_keywords(movie_id)
            
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
            
            # Process keywords - fetch keyword details and upsert
            keyword_db_ids = []
            for kw in keywords.keywords:
                # For now, create keyword with just ID and placeholder name
                # You may want to fetch keyword details from TMDB if needed
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
                f"Successfully processed movie: {movie_obj.title} (ID: {movie_obj.tmdb_id})"
            )
            
        except Exception as e:
            await job_log.log_error(
                db,
                job_id,
                f"Error processing movie {movie_id}: {str(e)}"
            )
            # Don't re-raise here, continue with other movies
            logger.error(f"Error processing movie {movie_id}: {str(e)}", exc_info=True)


# Job instance for scheduler
movie_discovery_job = MovieDiscoveryJob()