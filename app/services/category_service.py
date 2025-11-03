import json
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging

from app.core.redis import redis_client
from app.core.tmdb import get_tmdb_client
from app.crud.movie import movie as movie_crud
from sqlalchemy.ext.asyncio import AsyncSession

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
        cache_duration=2 * 60 * 60  # 2 hours
    ),
    "trending_week": CategoryConfig(
        name="Trending This Week", 
        tmdb_method="get_trending_movies_week",
        cache_duration=12 * 60 * 60  # 12 hours
    ),
    "popular": CategoryConfig(
        name="Popular",
        tmdb_method="get_popular_movies",
        cache_duration=24 * 60 * 60  # 24 hours
    ),
    "top_rated": CategoryConfig(
        name="Top Rated",
        tmdb_method="get_top_rated_movies", 
        cache_duration=48 * 60 * 60  # 48 hours
    ),
    "upcoming": CategoryConfig(
        name="Upcoming",
        tmdb_method="get_upcoming_movies",
        cache_duration=6 * 60 * 60  # 6 hours
    ),
    "now_playing": CategoryConfig(
        name="Now Playing",
        tmdb_method="get_now_playing_movies",
        cache_duration=4 * 60 * 60  # 4 hours
    ),
    # Language-based categories
    "bollywood": CategoryConfig(
        name="Bollywood",
        tmdb_method="get_bollywood_movies",
        cache_duration=24 * 60 * 60
    ),
    "tollywood": CategoryConfig(
        name="Tollywood", 
        tmdb_method="get_tollywood_movies",
        cache_duration=24 * 60 * 60
    ),
    "kollywood": CategoryConfig(
        name="Kollywood",
        tmdb_method="get_kollywood_movies", 
        cache_duration=24 * 60 * 60
    ),
    "hollywood": CategoryConfig(
        name="Hollywood",
        tmdb_method="get_hollywood_movies",
        cache_duration=24 * 60 * 60
    ),
}


class CategoryService:
    def __init__(self):
        pass  # No need to manage TMDB client instance anymore
    
    def _get_cache_key(self, category: str, page: int, **filters) -> str:
        if filters:
            filter_str = json.dumps(filters, sort_keys=True)
            filter_hash = hashlib.md5(filter_str.encode()).hexdigest()[:8]
            return f"category:{category}:filtered:{filter_hash}:page:{page}"
        return f"category:{category}:page:{page}"
    
    def _get_meta_cache_key(self, category: str, **filters) -> str:
        if filters:
            filter_str = json.dumps(filters, sort_keys=True)
            filter_hash = hashlib.md5(filter_str.encode()).hexdigest()[:8]
            return f"category:{category}:filtered:{filter_hash}:meta"
        return f"category:{category}:meta"
    
    async def _get_cached_page(self, cache_key: str) -> Optional[List[int]]:
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            logger.warning(f"Failed to get cached data for {cache_key}: {e}")
        return None
    
    async def _cache_page(self, cache_key: str, movie_ids: List[int], ttl: int):
        try:
            await redis_client.setex(
                cache_key, 
                ttl, 
                json.dumps(movie_ids)
            )
        except Exception as e:
            logger.warning(f"Failed to cache data for {cache_key}: {e}")
    
    async def _fetch_and_process_movies(
        self, 
        db: AsyncSession,
        tmdb_movies: List[Any],
        category: str
    ) -> List[int]:
        movie_ids = []
        tmdb_ids_to_fetch = []
        
        # Extract TMDB IDs from results
        tmdb_id_list = [movie.tmdb_id for movie in tmdb_movies if hasattr(movie, 'tmdb_id')]
        
        if not tmdb_id_list:
            logger.warning(f"No valid TMDB IDs found for category {category}")
            return movie_ids
        
        # Check which movies already exist in our DB
        existing_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_id_list)
        existing_tmdb_ids = {movie.tmdb_id: movie.id for movie in existing_movies}
        
        logger.info(f"Found {len(existing_movies)}/{len(tmdb_id_list)} movies in DB for {category}")
        
        # Add existing movie IDs to result
        for tmdb_id in tmdb_id_list:
            if tmdb_id in existing_tmdb_ids:
                movie_ids.append(existing_tmdb_ids[tmdb_id])
            else:
                tmdb_ids_to_fetch.append(tmdb_id)
        
        # Fetch missing movies from TMDB and store in DB
        if tmdb_ids_to_fetch:
            logger.info(f"Fetching {len(tmdb_ids_to_fetch)} missing movies from TMDB")
            tmdb_client = await get_tmdb_client()
            
            from app.utils.movie_processor import process_movie_batch
            
            # Process missing movies in batch
            batch_result = await process_movie_batch(
                db=db,
                tmdb_client=tmdb_client,
                movie_ids=tmdb_ids_to_fetch,
                job_id=None,  # No job tracking for category requests
                use_locks=False,  # Skip locks for category fetching
                cancel_event=None
            )
            
            # Get the newly created movies and add their IDs
            new_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids_to_fetch)
            for movie in new_movies:
                # Insert at correct position to maintain order
                try:
                    original_index = tmdb_id_list.index(movie.tmdb_id)
                    if original_index < len(movie_ids):
                        movie_ids.insert(original_index, movie.id)
                    else:
                        movie_ids.append(movie.id)
                except ValueError:
                    movie_ids.append(movie.id)
            
            logger.info(f"Successfully processed {batch_result.succeeded} movies for {category}")
        
        return movie_ids
    
    async def get_category_movies(
        self, 
        db: AsyncSession,
        category: str, 
        page: int = 1,
        **filters
    ) -> Tuple[List[int], Dict[str, Any]]:
        
        if category not in CATEGORY_CONFIGS:
            raise ValueError(f"Unknown category: {category}")
        
        config = CATEGORY_CONFIGS[category]
        cache_key = self._get_cache_key(category, page, **filters)
        meta_key = self._get_meta_cache_key(category, **filters)
        
        # Try to get from cache first
        cached_ids = await self._get_cached_page(cache_key)
        if cached_ids:
            logger.info(f"Cache hit for {category} page {page}")
            
            # Get metadata from cache
            try:
                cached_meta = await redis_client.get(meta_key)
                metadata = json.loads(cached_meta) if cached_meta else {}
            except:
                metadata = {}
            
            return cached_ids, metadata
        
        # Cache miss - fetch from TMDB
        logger.info(f"Cache miss for {category} page {page}, fetching from TMDB")
        tmdb_client = await get_tmdb_client()
        
        # Get TMDB method
        if not hasattr(tmdb_client, config.tmdb_method):
            raise ValueError(f"TMDB method {config.tmdb_method} not found")
        
        tmdb_method = getattr(tmdb_client, config.tmdb_method)
        
        # Call TMDB API
        try:
            tmdb_response = await tmdb_method(page=page, **filters)
            
            if not tmdb_response or not hasattr(tmdb_response, 'movies'):
                logger.warning(f"No movies found for {category} page {page}")
                return [], {}
            
            # Process movies and get our internal IDs
            movie_ids = await self._fetch_and_process_movies(
                db, tmdb_response.movies, category
            )
            
            # Prepare metadata
            metadata = {
                "total_pages": getattr(tmdb_response, 'total_pages', 1),
                "total_results": getattr(tmdb_response, 'total_results', len(movie_ids)),
                "current_page": page
            }
            
            # Cache the results
            await self._cache_page(cache_key, movie_ids, config.cache_duration)
            await redis_client.setex(
                meta_key, 
                config.cache_duration, 
                json.dumps(metadata)
            )
            
            logger.info(f"Cached {len(movie_ids)} movie IDs for {category} page {page}")
            return movie_ids, metadata
            
        except Exception as e:
            logger.error(f"Failed to fetch {category} from TMDB: {e}")
            raise
    
    async def get_available_categories(self) -> List[Dict[str, str]]:
        """Get list of available categories."""
        return [
            {
                "key": key,
                "name": config.name
            }
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