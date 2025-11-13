import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud import job_log, movie
from app.models.movie import Movie, MovieCreate
from app.services.tmdb_client.client import TMDBClient
from app.services.tmdb_client.models import MovieItem
from app.utils.processors import genre_processor, keyword_processor
from app.utils.rate_limiter import rate_limited_call, tmdb_rate_limiter

logger = logging.getLogger(__name__)


@dataclass
class BatchProcessResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped_locked: int = 0
    skipped_existing: int = 0  # For insert-only mode


async def hydrate_movie_full(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    movie_obj: Movie,
    hydration_source: str = "background",
    job_id: int | None = None,
) -> Movie | None:
    """Hydrate an existing movie with full details from TMDB.

    Fetches runtime, budget, revenue, status, genres, and keywords
    and updates the movie record. Sets is_hydrated=True on success.

    Args:
        db: Database session
        tmdb_client: TMDB API client
        movie_obj: Existing Movie object to hydrate
        hydration_source: Source of hydration ('background', 'user_request', 'job')
        job_id: Optional job ID for logging

    Returns:
        Updated Movie object if successful, None if failed
    """
    try:
        # Refresh object to ensure it's attached to session and attributes are loaded
        await db.refresh(movie_obj)
        tmdb_id = movie_obj.tmdb_id

        # Fetch movie details and keywords from TMDB with rate limiting
        movie_details, keywords = await asyncio.gather(
            rate_limited_call(
                tmdb_rate_limiter,
                lambda: tmdb_client.get_movie_by_id(tmdb_id),
            ),
            rate_limited_call(
                tmdb_rate_limiter,
                lambda: tmdb_client.get_movie_keywords(tmdb_id),
            ),
        )

        if not movie_details:
            if job_id:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"Could not fetch details for movie {tmdb_id}",
                )
            logger.warning(f"Failed to fetch details for movie {tmdb_id}")
            return None

        # Process related entities (genres and keywords)
        genre_ids = await genre_processor.process_genres(
            db, movie_details.genres, job_id
        )
        keyword_ids = await keyword_processor.process_keywords(db, keywords, job_id)

        # Prepare update data with full hydration
        movie_create = MovieCreate(
            tmdb_id=tmdb_id,  # Required field for MovieCreate
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
            is_hydrated=True,
            last_hydrated_at=datetime.now(),
            hydration_source=hydration_source,
        )

        # Update movie with relationships (upsert will update existing record)
        updated_movie = await movie.upsert_movie_with_relationships(
            db,
            movie_create=movie_create,
            genre_ids=genre_ids,
            keyword_ids=keyword_ids,
            commit=False,
        )

        await db.commit()

        if job_id:
            await job_log.log_info(
                db,
                job_id,
                f"Successfully hydrated movie {tmdb_id} ({hydration_source})",
            )

        logger.info(f"Successfully hydrated movie {tmdb_id} from {hydration_source}")
        return updated_movie

    except Exception as e:
        await db.rollback()

        if job_id:
            await job_log.log_error(
                db, job_id, f"Error hydrating movie {tmdb_id}: {e!s}"
            )
        logger.error(f"Error hydrating movie {tmdb_id}: {e!s}", exc_info=True)
        return None


async def hydrate_movie_by_tmdb_id(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    tmdb_id: int,
    hydration_source: str = "background",
    job_id: int | None = None,
) -> Movie | None:
    """Hydrate a movie by its TMDB ID.

    Looks up the movie in the database and hydrates it if found.
    If not found, returns None.

    Args:
        db: Database session
        tmdb_client: TMDB API client
        tmdb_id: TMDB movie ID
        hydration_source: Source of hydration
        job_id: Optional job ID for logging

    Returns:
        Hydrated Movie object if successful, None if not found or failed
    """
    movie_obj = await movie.get_by_tmdb_id(db, tmdb_id)
    if not movie_obj:
        logger.warning(f"Movie with tmdb_id={tmdb_id} not found in database")
        return None

    return await hydrate_movie_full(
        db, tmdb_client, movie_obj, hydration_source, job_id
    )


# PROCESSOR 1: Lightweight Insert + Queue (for Endpoints)


async def insert_from_list_and_queue(
    db: AsyncSession,
    tmdb_movies: list[MovieItem],
    *,
    queue_for_hydration: bool = True,
) -> list[Movie]:
    """Processor 1: Insert movies from TMDB list + queue for background hydration.

    This is the FAST processor for endpoints (search, discover, categories).
    - Inserts minimal data immediately (from MovieItem)
    - Optionally queues for background hydration (fire-and-forget)
    - Returns immediately with partial data

    Args:
        db: Database session
        tmdb_movies: List of MovieItem from TMDB list endpoints
        queue_for_hydration: Whether to queue for background hydration

    Returns:
        List of Movie objects with partial data (is_hydrated=False)
    """
    if not tmdb_movies:
        return []

    # Insert movies with partial data
    inserted_movies = await movie.insert_movies_from_tmdb_list_batch(
        db, tmdb_movies, commit=True
    )

    # Queue for background hydration (fire-and-forget)
    if queue_for_hydration:
        # Lazy import to avoid circular dependency with hydration_service
        from app.services.hydration_service import hydration_service

        tmdb_ids = [m.tmdb_id for m in tmdb_movies]
        hydration_service.queue_movies_batch_background(tmdb_ids)

    return inserted_movies


# PROCESSOR 2: Full Insert (for Discovery Job & Background Worker)


async def fetch_and_insert_full(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    tmdb_id: int,
    hydration_source: str = "job",
    job_id: int | None = None,
) -> Movie | None:
    """Processor 2: Fetch full movie details and INSERT only (skip if exists).

    This processor is for:
    - movie_discovery job (adds new movies)
    - background hydration worker (hydrates existing partial movies)

    Rules:
    - Checks if movie exists
    - If exists and already hydrated, SKIP
    - If exists but not hydrated, UPDATE to hydrated
    - If doesn't exist, INSERT with full data

    Args:
        db: Database session
        tmdb_client: TMDB API client
        tmdb_id: TMDB movie ID
        hydration_source: Source identifier ('job', 'background')
        job_id: Optional job ID for logging

    Returns:
        Movie object if successful, None if failed or skipped
    """
    try:
        # Check if movie already exists
        existing_movie = await movie.get_by_tmdb_id(db, tmdb_id)

        if existing_movie:
            # Access attributes while object is still in session
            # This ensures attributes are loaded before any operations that might detach
            is_hydrated = existing_movie.is_hydrated
            movie_tmdb_id = existing_movie.tmdb_id

            # If already hydrated, skip
            if is_hydrated:
                logger.debug(f"Movie {movie_tmdb_id} already hydrated, skipping")
                return existing_movie

            # If not hydrated, update it
            # Pass tmdb_id directly instead of relying on object attribute
            return await hydrate_movie_full(
                db, tmdb_client, existing_movie, hydration_source, job_id
            )

        # Movie doesn't exist, fetch and insert with full data
        movie_details, keywords = await asyncio.gather(
            rate_limited_call(
                tmdb_rate_limiter, lambda: tmdb_client.get_movie_by_id(tmdb_id)
            ),
            rate_limited_call(
                tmdb_rate_limiter, lambda: tmdb_client.get_movie_keywords(tmdb_id)
            ),
        )

        if not movie_details:
            if job_id:
                await job_log.log_warning(
                    db, job_id, f"Could not fetch details for movie {tmdb_id}"
                )
            return None

        # Process genres and keywords
        genre_ids = await genre_processor.process_genres(
            db, movie_details.genres, job_id
        )
        keyword_ids = await keyword_processor.process_keywords(db, keywords, job_id)

        # Create with full data
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
            is_hydrated=True,
            last_hydrated_at=datetime.now(),
            hydration_source=hydration_source,
        )

        # Save with relationships
        movie_obj = await movie.upsert_movie_with_relationships(
            db,
            movie_create=movie_create,
            genre_ids=genre_ids,
            keyword_ids=keyword_ids,
            commit=False,
        )

        await db.commit()
        return movie_obj

    except Exception as e:
        await db.rollback()
        if job_id:
            await job_log.log_error(
                db, job_id, f"Error processing movie {tmdb_id}: {e!s}"
            )
        logger.error(
            f"Error in fetch_and_insert_full for movie {tmdb_id}: {e!s}", exc_info=True
        )
        return None


# PROCESSOR 3: Full Upsert (for Change Tracking Job)


async def fetch_and_upsert_full(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    tmdb_id: int,
    job_id: int | None = None,
) -> Movie | None:
    """Processor 3: Fetch full details and ALWAYS update/insert.

    This processor is ONLY for change_tracking job.

    Rules:
    - ALWAYS fetches fresh data from TMDB
    - ALWAYS updates if exists, inserts if not
    - Sets is_hydrated=True with source='job'

    Args:
        db: Database session
        tmdb_client: TMDB API client
        tmdb_id: TMDB movie ID
        job_id: Optional job ID for logging

    Returns:
        Movie object if successful, None if failed
    """
    try:
        # Fetch full details (always, even if exists)
        movie_details, keywords = await asyncio.gather(
            rate_limited_call(
                tmdb_rate_limiter, lambda: tmdb_client.get_movie_by_id(tmdb_id)
            ),
            rate_limited_call(
                tmdb_rate_limiter, lambda: tmdb_client.get_movie_keywords(tmdb_id)
            ),
        )

        if not movie_details:
            if job_id:
                await job_log.log_warning(
                    db, job_id, f"Could not fetch details for movie {tmdb_id}"
                )
            return None

        # Process genres and keywords
        genre_ids = await genre_processor.process_genres(
            db, movie_details.genres, job_id
        )
        keyword_ids = await keyword_processor.process_keywords(db, keywords, job_id)

        # Create/update with full data
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
            is_hydrated=True,
            last_hydrated_at=datetime.now(),
            hydration_source="job",
        )

        movie_obj = await movie.upsert_movie_with_relationships(
            db,
            movie_create=movie_create,
            genre_ids=genre_ids,
            keyword_ids=keyword_ids,
            commit=False,
        )

        await db.commit()
        return movie_obj

    except Exception as e:
        await db.rollback()
        if job_id:
            await job_log.log_error(
                db, job_id, f"Error processing movie {tmdb_id}: {e!s}"
            )
        logger.error(
            f"Error in fetch_and_upsert_full for movie {tmdb_id}: {e!s}", exc_info=True
        )
        return None
