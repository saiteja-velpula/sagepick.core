import asyncio
import logging
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.tmdb_client.client import TMDBClient
from app.crud import movie, genre, keyword, job_log
from app.models.movie import Movie, MovieCreate

logger = logging.getLogger(__name__)


async def process_tmdb_movie(db: AsyncSession, tmdb_client: TMDBClient, movie_id: int, job_id: Optional[int] = None) -> Optional[Movie]:
    try:
        if job_id:
            await job_log.log_info(
                db,
                job_id,
                f"Processing movie {movie_id}"
            )
        
        # Fetch movie details from TMDB
        movie_details = await tmdb_client.get_movie_by_id(movie_id)
        if not movie_details:
            if job_id:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"Could not fetch details for movie {movie_id}"
                )
            return None
        
        # Fetch movie keywords from TMDB
        keywords = await tmdb_client.get_movie_keywords(movie_id)
        
        # Add delay after API calls
        await asyncio.sleep(0.25)  # API_DELAY
        
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
        if keywords and keywords.keywords:
            for kw in keywords.keywords:
                keyword_obj = await keyword.upsert_keyword(
                    db,
                    keyword_id=kw.id,
                    name=kw.name
                )
                keyword_db_ids.append(keyword_obj.id)
        
        # Create movie object with full details
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
        
        if job_id:
            await job_log.log_info(
                db,
                job_id,
                f"Successfully processed movie: {movie_obj.title} (ID: {movie_obj.tmdb_id})"
            )
        
        return movie_obj
        
    except Exception as e:
        if job_id:
            await job_log.log_error(
                db,
                job_id,
                f"Error processing movie {movie_id}: {str(e)}"
            )
        logger.error(f"Error processing movie {movie_id}: {str(e)}", exc_info=True)
        return None


async def process_movie_batch(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    movie_ids: List[int],
    job_id: Optional[int] = None
) -> int:
    processed_count = 0
    
    for movie_id in movie_ids:
        result = await process_tmdb_movie(db, tmdb_client, movie_id, job_id)
        if result:
            processed_count += 1
    
    if job_id:
        await job_log.log_info(
            db,
            job_id,
            f"Batch processing complete: {processed_count}/{len(movie_ids)} movies processed successfully"
        )
    
    return processed_count