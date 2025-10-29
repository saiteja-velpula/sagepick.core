import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.genre import genre
from app.utils.cache.genre_cache import genre_cache

logger = logging.getLogger(__name__)


class GenreProcessor:
    def __init__(self):
        self.cache = genre_cache

    async def process_genres(
        self, db: AsyncSession, tmdb_genres: List[Any], job_id: Optional[int] = None
    ) -> List[int]:
        """
        Process TMDB genres and return internal IDs.

        Args:
            db: Database session
            tmdb_genres: List of TMDB genre objects
            job_id: Optional job ID for logging

        Returns:
            List of internal genre IDs
        """
        if not tmdb_genres:
            return []

        # Initialize cache
        cache_map = await self.cache.get_map(db)

        genre_ids = []
        uncached_genres = []
        seen_tmdb_ids = set()

        # Separate cached and uncached genres
        for genre_data in tmdb_genres:
            if genre_data.id in seen_tmdb_ids:
                continue
            seen_tmdb_ids.add(genre_data.id)

            cached_id = cache_map.get(genre_data.id)
            if cached_id is not None:
                genre_ids.append(cached_id)
            else:
                uncached_genres.append(
                    {"tmdb_id": genre_data.id, "name": genre_data.name}
                )

        # Process uncached genres
        if uncached_genres:
            processed_ids = await self._process_uncached_genres(
                db, uncached_genres, job_id
            )
            genre_ids.extend(processed_ids)

        return genre_ids

    async def _process_uncached_genres(
        self,
        db: AsyncSession,
        uncached_genres: List[Dict[str, Any]],
        job_id: Optional[int] = None,
    ) -> List[int]:
        """Process genres not found in cache."""
        try:
            # Try batch processing first
            tmdb_to_id_mapping = await genre.upsert_genres_batch(
                db, uncached_genres, commit=False, flush=True
            )

            # Update cache
            self.cache.set_batch(tmdb_to_id_mapping)

            # Return IDs in order
            genre_ids = []
            for item in uncached_genres:
                internal_id = tmdb_to_id_mapping.get(item["tmdb_id"])
                if internal_id is not None:
                    genre_ids.append(internal_id)
                else:
                    logger.error(f"Failed to get ID for genre {item['tmdb_id']}")

            return genre_ids

        except Exception as e:
            logger.error(f"Batch genre processing failed: {e}")
            return await self._process_genres_individually(db, uncached_genres, job_id)

    async def _process_genres_individually(
        self,
        db: AsyncSession,
        genre_data: List[Dict[str, Any]],
        job_id: Optional[int] = None,
    ) -> List[int]:
        """Fallback to individual genre processing."""
        genre_ids = []

        for item in genre_data:
            try:
                genre_obj = await genre.upsert_genre(
                    db,
                    genre_id=item["tmdb_id"],
                    name=item["name"],
                    commit=False,
                    flush=True,
                )

                if genre_obj.id is not None:
                    self.cache.set(item["tmdb_id"], genre_obj.id)
                    genre_ids.append(genre_obj.id)
                else:
                    logger.error(f"Genre {item['tmdb_id']} has no ID after flush")

            except Exception as e:
                logger.error(f"Failed to process genre {item['tmdb_id']}: {e}")

        return genre_ids


# Global processor instance
genre_processor = GenreProcessor()
