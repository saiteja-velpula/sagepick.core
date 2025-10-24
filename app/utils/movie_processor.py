import asyncio
import logging
from dataclasses import dataclass
from time import monotonic
from typing import Awaitable, Callable, Dict, List, Optional, TypeVar
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.services.tmdb_client.client import TMDBClient
from app.crud import movie, genre, keyword, job_log, job_status
from app.core.redis import redis_client
from app.core.settings import settings
from app.models.movie import Movie, MovieCreate
from app.models.genre import Genre

logger = logging.getLogger(__name__)


@dataclass
class BatchProcessResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped_locked: int = 0


@dataclass
class _LookupCaches:
    genres: Dict[int, int]
    keywords: Dict[int, int]


class _GenreCache:
    """Process-wide genre cache hydrated from the database once."""

    def __init__(self) -> None:
        self._loaded = False
        self._lock = asyncio.Lock()
        self._map: Dict[int, int] = {}

    async def get_map(self, db: AsyncSession) -> Dict[int, int]:
        if self._loaded:
            return self._map

        async with self._lock:
            if not self._loaded:
                result = await db.execute(select(Genre.tmdb_id, Genre.id))
                rows = result.all()
                self._map = {int(tmdb_id): int(db_id) for tmdb_id, db_id in rows}
                self._loaded = True
        return self._map

    def set(self, tmdb_id: int, local_id: int) -> None:
        self._map[tmdb_id] = local_id


class _KeywordCache:
    """Redis-backed keyword cache with an in-process mirror."""

    _REDIS_HASH_KEY = "sagepick:tmdb:keywords"

    def __init__(self) -> None:
        self._loaded = False
        self._lock = asyncio.Lock()
        self._map: Dict[int, int] = {}

    async def get_map(self) -> Dict[int, int]:
        if self._loaded:
            return self._map

        async with self._lock:
            if not self._loaded:
                await self._load()
        return self._map

    async def _load(self) -> None:
        try:
            await redis_client.initialize()
        except Exception as exc:
            logger.warning("Keyword cache unavailable; proceeding without Redis: %s", exc)
            self._map = {}
            self._loaded = True
            return

        data = await redis_client.hgetall(self._REDIS_HASH_KEY)
        self._map = {int(k): int(v) for k, v in data.items()}
        self._loaded = True

    async def set(self, tmdb_id: int, local_id: int) -> None:
        already_known = tmdb_id in self._map
        self._map[tmdb_id] = local_id

        if not redis_client.redis:
            return

        if already_known or len(self._map) <= settings.TMDB_KEYWORD_CACHE_MAX_ENTRIES:
            await redis_client.hset(self._REDIS_HASH_KEY, tmdb_id, local_id)
        else:
            logger.debug(
                "Skipping Redis keyword persist; cache limit %s reached",
                settings.TMDB_KEYWORD_CACHE_MAX_ENTRIES
            )


class _AsyncRateLimiter:
    """Simple async limiter that spaces calls to respect TMDB quotas."""

    def __init__(self, max_per_second: int):
        self._interval = 1.0 / max(1, max_per_second)
        self._lock = asyncio.Lock()
        self._last_acquire = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = monotonic()
            elapsed = now - self._last_acquire
            if elapsed < self._interval:
                await asyncio.sleep(self._interval - elapsed)
            self._last_acquire = monotonic()


_T = TypeVar("_T")
_genre_cache = _GenreCache()
_keyword_cache = _KeywordCache()
_rate_limiter = _AsyncRateLimiter(settings.TMDB_MAX_REQUESTS_PER_SECOND)


async def _call_with_rate_limit(
    limiter: _AsyncRateLimiter,
    coro_factory: Callable[[], Awaitable[_T]]
) -> _T:
    await limiter.acquire()
    return await coro_factory()


async def process_tmdb_movie(
    db: AsyncSession,
    tmdb_client: TMDBClient,
    movie_id: int,
    caches: _LookupCaches,
    job_id: Optional[int] = None,
    *,
    rate_limiter: Optional[_AsyncRateLimiter] = None
) -> Optional[Movie]:
    try:
        limiter = rate_limiter or _rate_limiter

        movie_details, keywords = await asyncio.gather(
            _call_with_rate_limit(limiter, lambda: tmdb_client.get_movie_by_id(movie_id)),
            _call_with_rate_limit(limiter, lambda: tmdb_client.get_movie_keywords(movie_id)),
        )

        if not movie_details:
            if job_id:
                await job_log.log_warning(
                    db,
                    job_id,
                    f"Could not fetch details for movie {movie_id}"
                )
            return None

        # Process genres
        genre_ids = []
        if movie_details.genres:
            for genre_data in movie_details.genres:
                cached_id = caches.genres.get(genre_data.id)
                if cached_id is None:
                    genre_obj = await genre.upsert_genre(
                        db,
                        genre_id=genre_data.id,
                        name=genre_data.name,
                        commit=False
                    )
                    cached_id = genre_obj.id
                    caches.genres[genre_data.id] = cached_id
                    _genre_cache.set(genre_data.id, cached_id)
                genre_ids.append(cached_id)

        # Process keywords
        keyword_db_ids = []
        if keywords and keywords.keywords:
            for kw in keywords.keywords:
                cached_id = caches.keywords.get(kw.id)
                if cached_id is None:
                    keyword_obj = await keyword.upsert_keyword(
                        db,
                        keyword_id=kw.id,
                        name=kw.name,
                        commit=False
                    )
                    cached_id = keyword_obj.id
                    caches.keywords[kw.id] = cached_id
                    await _keyword_cache.set(kw.id, cached_id)
                keyword_db_ids.append(cached_id)

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
            keyword_ids=keyword_db_ids,
            commit=False
        )

        await db.flush()
        
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
    use_locks: bool = False,
    cancel_event: Optional[asyncio.Event] = None
) -> BatchProcessResult:
    result_summary = BatchProcessResult()
    cancellation_noted = False
    genre_seed = await _genre_cache.get_map(db)
    keyword_seed = await _keyword_cache.get_map()
    caches = _LookupCaches(genres=genre_seed, keywords=keyword_seed)
    processed_delta = 0
    failed_delta = 0

    for movie_id in movie_ids:
        if cancel_event and cancel_event.is_set():
            if job_id and not cancellation_noted:
                await job_log.log_warning(
                    db,
                    job_id,
                    "Cancellation requested; stopping remaining movie processing"
                )
                cancellation_noted = True
            break

        result_summary.attempted += 1

        lock_acquired = True
        if use_locks:
            try:
                lock_acquired = await redis_client.acquire_movie_lock(movie_id)
            except Exception as exc: 
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
                failed_delta += 1
            continue

        try:
            processed_movie = await process_tmdb_movie(
                db,
                tmdb_client,
                movie_id,
                caches,
                job_id=job_id
            )
            if processed_movie:
                result_summary.succeeded += 1
                if job_id:
                    processed_delta += 1
            else:
                result_summary.failed += 1
                if job_id:
                    failed_delta += 1
        except Exception as exc: 
            result_summary.failed += 1
            if job_id:
                await job_log.log_error(
                    db,
                    job_id,
                    f"Unhandled error processing movie {movie_id}: {str(exc)}"
                )
                failed_delta += 1
            logger.error("Unhandled error processing movie %s: %s", movie_id, exc, exc_info=True)
        finally:
            if use_locks and lock_acquired:
                try:
                    await redis_client.release_movie_lock(movie_id)
                except Exception as exc: 
                    logger.error("Failed to release lock for movie %s: %s", movie_id, exc, exc_info=True)

    if job_id and (processed_delta or failed_delta):
        await job_status.increment_counts(
            db,
            job_id,
            processed_delta=processed_delta,
            failed_delta=failed_delta
        )

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