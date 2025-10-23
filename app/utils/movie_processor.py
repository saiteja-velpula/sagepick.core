import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.tmdb_client.client import TMDBClient
from app.crud import movie, genre, keyword, job_log, job_status
from app.core.redis import redis_client
from app.models.movie import Movie, MovieCreate

logger = logging.getLogger(__name__)


@dataclass
class BatchProcessResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped_locked: int = 0


async def process_tmdb_movie(db: AsyncSession, tmdb_client: TMDBClient, movie_id: int, job_id: Optional[int] = None) -> Optional[Movie]:
    try:
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
    job_id: Optional[int] = None,
    *,
    use_locks: bool = False
) -> BatchProcessResult:
    result_summary = BatchProcessResult()

    for movie_id in movie_ids:
        result_summary.attempted += 1

        lock_acquired = True
        if use_locks:
            try:
                lock_acquired = await redis_client.acquire_movie_lock(movie_id)
            except Exception as exc:  # pragma: no cover - defensive logging
                lock_acquired = False
                logger.error("Failed to acquire lock for movie %s: %s", movie_id, exc, exc_info=True)

        if not lock_acquired:
            result_summary.failed += 1
            result_summary.skipped_locked += 1
            if job_id:
                await job_log.log_info(
                    db,
                    job_id,
                    f"Skipped movie {movie_id} due to existing lock"
                )
                await job_status.increment_counts(db, job_id, failed_delta=1)
            continue

        try:
            processed_movie = await process_tmdb_movie(db, tmdb_client, movie_id, job_id)
            if processed_movie:
                result_summary.succeeded += 1
                if job_id:
                    await job_status.increment_counts(db, job_id, processed_delta=1)
            else:
                result_summary.failed += 1
                if job_id:
                    await job_status.increment_counts(db, job_id, failed_delta=1)
        except Exception as exc:  # pragma: no cover - defensive logging
            result_summary.failed += 1
            if job_id:
                await job_log.log_error(
                    db,
                    job_id,
                    f"Unhandled error processing movie {movie_id}: {str(exc)}"
                )
                await job_status.increment_counts(db, job_id, failed_delta=1)
            logger.error("Unhandled error processing movie %s: %s", movie_id, exc, exc_info=True)
        finally:
            if use_locks and lock_acquired:
                try:
                    await redis_client.release_movie_lock(movie_id)
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.error("Failed to release lock for movie %s: %s", movie_id, exc, exc_info=True)

    if job_id:
        await job_log.log_info(
            db,
            job_id,
            (
                "Batch processing complete: "
                f"{result_summary.succeeded}/{result_summary.attempted} succeeded, "
                f"{result_summary.failed} failed"
                + (
                    f" ({result_summary.skipped_locked} skipped due to locks)"
                    if result_summary.skipped_locked
                    else ""
                )
            )
        )

    return result_summary