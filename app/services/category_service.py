import hashlib
import json
import logging
from dataclasses import dataclass
from math import ceil
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis import redis_client
from app.core.tmdb import get_tmdb_client
from app.crud.movie import movie as movie_crud
from app.utils.movie_processor import insert_from_list_and_queue

logger = logging.getLogger(__name__)


@dataclass
class CategoryConfig:
    name: str
    tmdb_method: str
    cache_duration: int


# Category configurations
CATEGORY_CONFIGS = {
    "trending_day": CategoryConfig(
        name="Trending Today",
        tmdb_method="get_trending_movies_day",
        cache_duration=2 * 60 * 60,  # 2 hours
    ),
    "trending_week": CategoryConfig(
        name="Trending This Week",
        tmdb_method="get_trending_movies_week",
        cache_duration=12 * 60 * 60,  # 12 hours
    ),
    "popular": CategoryConfig(
        name="Popular",
        tmdb_method="get_popular_movies",
        cache_duration=24 * 60 * 60,  # 24 hours
    ),
    "top_rated": CategoryConfig(
        name="Top Rated",
        tmdb_method="get_top_rated_movies",
        cache_duration=48 * 60 * 60,  # 48 hours
    ),
    "upcoming": CategoryConfig(
        name="Upcoming",
        tmdb_method="get_upcoming_movies",
        cache_duration=6 * 60 * 60,  # 6 hours
    ),
    "now_playing": CategoryConfig(
        name="Now Playing",
        tmdb_method="get_now_playing_movies",
        cache_duration=4 * 60 * 60,  # 4 hours
    ),
    # Language-based categories
    "bollywood": CategoryConfig(
        name="Bollywood",
        tmdb_method="get_bollywood_movies",
        cache_duration=24 * 60 * 60,
    ),
    "tollywood": CategoryConfig(
        name="Tollywood",
        tmdb_method="get_tollywood_movies",
        cache_duration=24 * 60 * 60,
    ),
    "kollywood": CategoryConfig(
        name="Kollywood",
        tmdb_method="get_kollywood_movies",
        cache_duration=24 * 60 * 60,
    ),
    "hollywood": CategoryConfig(
        name="Hollywood",
        tmdb_method="get_hollywood_movies",
        cache_duration=24 * 60 * 60,
    ),
}


TMDB_PAGE_SIZE = 20


class CategoryService:
    def __init__(self):
        pass  # No need to manage TMDB client instance anymore

    def _get_cache_key(self, category: str, page: int, **filters) -> str:
        if filters:
            filter_str = json.dumps(filters, sort_keys=True)
            filter_hash = hashlib.md5(  # nosec B324  # noqa: S324
                filter_str.encode()
            ).hexdigest()[:8]
            return f"category:{category}:filtered:{filter_hash}:page:{page}"
        return f"category:{category}:page:{page}"

    def _get_meta_cache_key(self, category: str, **filters) -> str:
        if filters:
            filter_str = json.dumps(filters, sort_keys=True)
            filter_hash = hashlib.md5(  # nosec B324  # noqa: S324
                filter_str.encode()
            ).hexdigest()[:8]
            return f"category:{category}:filtered:{filter_hash}:meta"
        return f"category:{category}:meta"

    async def _get_cached_page(self, cache_key: str) -> list[int] | None:
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            logger.warning(f"Failed to get cached data for {cache_key}: {e}")
        return None

    async def _cache_page(self, cache_key: str, movie_ids: list[int], ttl: int):
        try:
            await redis_client.setex(cache_key, ttl, json.dumps(movie_ids))
        except Exception as e:
            logger.warning(f"Failed to cache data for {cache_key}: {e}")

    async def _get_tmdb_page(
        self,
        db: AsyncSession,
        category: str,
        tmdb_page: int,
        config: CategoryConfig,
        **filters,
    ) -> tuple[list[int], dict[str, Any]]:
        cache_key = self._get_cache_key(category, tmdb_page, **filters)
        meta_key = self._get_meta_cache_key(category, **filters)

        cached_ids = await self._get_cached_page(cache_key)
        if cached_ids is not None:
            metadata: dict[str, Any] = {}
            try:
                cached_meta = await redis_client.get(meta_key)
                if cached_meta:
                    metadata = json.loads(cached_meta)
            except Exception as e:
                logger.warning(f"Failed to load cached metadata for {category}: {e}")

            metadata.setdefault("tmdb_total_pages", metadata.get("total_pages"))
            metadata.setdefault("tmdb_page_size", TMDB_PAGE_SIZE)
            metadata.setdefault("total_results", len(cached_ids))
            metadata["tmdb_page"] = tmdb_page
            return cached_ids, metadata

        logger.info(
            f"Cache miss for {category} TMDB page {tmdb_page}, fetching from TMDB"
        )
        tmdb_client = await get_tmdb_client()

        if not hasattr(tmdb_client, config.tmdb_method):
            raise ValueError(f"TMDB method {config.tmdb_method} not found")

        tmdb_method = getattr(tmdb_client, config.tmdb_method)

        tmdb_response = await tmdb_method(page=tmdb_page, **filters)

        if not tmdb_response or not hasattr(tmdb_response, "movies"):
            logger.warning(f"No movies found for {category} TMDB page {tmdb_page}")
            metadata = {
                "total_results": getattr(tmdb_response, "total_results", 0),
                "tmdb_total_pages": getattr(tmdb_response, "total_pages", 0),
                "tmdb_page_size": TMDB_PAGE_SIZE,
                "tmdb_page": tmdb_page,
            }
            await redis_client.setex(
                meta_key, config.cache_duration, json.dumps(metadata)
            )
            return [], metadata

        movie_ids = await self._fetch_and_process_movies(
            db, tmdb_response.movies, category
        )

        metadata = {
            "total_results": tmdb_response.pagination.total_results,
            "tmdb_total_pages": tmdb_response.pagination.total_pages,
            "tmdb_page_size": TMDB_PAGE_SIZE,
            "tmdb_page": tmdb_page,
        }
        metadata["total_pages"] = metadata["tmdb_total_pages"]

        await self._cache_page(cache_key, movie_ids, config.cache_duration)
        await redis_client.setex(meta_key, config.cache_duration, json.dumps(metadata))

        logger.info(
            f"Cached {len(movie_ids)} movie IDs for {category} TMDB page {tmdb_page}"
        )
        return movie_ids, metadata

    async def _fetch_and_process_movies(
        self, db: AsyncSession, tmdb_movies: list[Any], category: str
    ) -> list[int]:
        """Use Processor 1 to insert movies lightweight and queue for hydration."""
        # Extract TMDB IDs from results
        tmdb_id_list = [
            movie.tmdb_id for movie in tmdb_movies if hasattr(movie, "tmdb_id")
        ]

        if not tmdb_id_list:
            logger.warning(f"No valid TMDB IDs found for category {category}")
            return []

        # Check which movies already exist in our DB
        existing_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_id_list)
        existing_tmdb_ids_set = {movie.tmdb_id for movie in existing_movies}

        logger.info(
            "Found %d/%d movies in DB for %s",
            len(existing_movies),
            len(tmdb_id_list),
            category,
        )

        # Find missing movies
        missing_movies = [
            movie
            for movie in tmdb_movies
            if hasattr(movie, "tmdb_id") and movie.tmdb_id not in existing_tmdb_ids_set
        ]

        # Use Processor 1: Insert lightweight + queue for background hydration
        if missing_movies:
            logger.info(
                f"Inserting {len(missing_movies)} missing movies for {category}"
            )
            await insert_from_list_and_queue(
                db, missing_movies, queue_for_hydration=True
            )

        # Get all movies (both existing and newly inserted)
        all_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_id_list)
        movie_id_map = {movie.tmdb_id: movie.id for movie in all_movies}

        # Return IDs in original order
        movie_ids = [
            movie_id_map[tmdb_id] for tmdb_id in tmdb_id_list if tmdb_id in movie_id_map
        ]

        logger.info(f"Processed {len(movie_ids)} movies for {category}")
        return movie_ids

    async def get_category_movies(
        self,
        db: AsyncSession,
        category: str,
        page: int = 1,
        per_page: int = TMDB_PAGE_SIZE,
        **filters,
    ) -> tuple[list[int], dict[str, Any]]:
        if category not in CATEGORY_CONFIGS:
            raise ValueError(f"Unknown category: {category}")

        config = CATEGORY_CONFIGS[category]
        per_page = max(1, min(per_page, 100))

        start_index = (page - 1) * per_page
        end_index = start_index + per_page

        tmdb_page_start = start_index // TMDB_PAGE_SIZE + 1
        tmdb_page_end = max(tmdb_page_start, (end_index - 1) // TMDB_PAGE_SIZE + 1)

        aggregated_ids: list[int] = []
        total_results: int | None = None
        tmdb_total_pages: int | None = None

        for tmdb_page in range(tmdb_page_start, tmdb_page_end + 1):
            try:
                page_ids, metadata = await self._get_tmdb_page(
                    db, category, tmdb_page, config, **filters
                )
            except Exception as exc:
                logger.error(f"Failed to fetch {category} TMDB page {tmdb_page}: {exc}")
                raise

            if total_results is None:
                total_results = metadata.get("total_results", 0)
                tmdb_total_pages = metadata.get("tmdb_total_pages")
                if total_results is not None and start_index >= total_results:
                    return [], {
                        "total_results": total_results,
                        "tmdb_total_pages": tmdb_total_pages or 0,
                        "tmdb_page_size": TMDB_PAGE_SIZE,
                    }

            if tmdb_total_pages is not None and tmdb_page > tmdb_total_pages:
                break

            if not page_ids:
                break

            page_start_index = (tmdb_page - 1) * TMDB_PAGE_SIZE
            slice_start = max(start_index - page_start_index, 0)
            slice_end = max(min(end_index - page_start_index, TMDB_PAGE_SIZE), 0)

            if slice_start < slice_end:
                aggregated_ids.extend(page_ids[slice_start:slice_end])

            if len(aggregated_ids) >= per_page:
                break

            if tmdb_total_pages is not None and tmdb_page >= tmdb_total_pages:
                break

        aggregated_ids = aggregated_ids[:per_page]

        if total_results is None:
            total_results = len(aggregated_ids)

        total_pages = ceil(total_results / per_page) if total_results else 0

        response_metadata = {
            "total_results": total_results,
            "total_pages": total_pages,
            "tmdb_total_pages": tmdb_total_pages,
            "tmdb_page_size": TMDB_PAGE_SIZE,
        }

        return aggregated_ids, response_metadata

    async def get_available_categories(self) -> list[dict[str, str]]:
        """Get list of available categories."""
        return [
            {"key": key, "name": config.name}
            for key, config in CATEGORY_CONFIGS.items()
        ]

    async def invalidate_category_cache(self, category: str):
        """Invalidate all cached pages for a category."""
        try:
            pattern = f"category:{category}:*"
            keys = await redis_client.keys(pattern)
            if keys:
                await redis_client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} cache entries for {category}")
        except Exception as e:
            logger.warning(f"Failed to invalidate cache for {category}: {e}")


# Global service instance
category_service = CategoryService()
