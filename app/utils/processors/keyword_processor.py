import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.keyword import keyword
from app.utils.cache.keyword_cache import keyword_cache

logger = logging.getLogger(__name__)


class KeywordProcessor:
    def __init__(self):
        self.cache = keyword_cache

    async def process_keywords(
        self, db: AsyncSession, tmdb_keywords: Any, job_id: Optional[int] = None
    ) -> List[int]:
        """
        Process TMDB keywords and return internal IDs.

        Args:
            db: Database session
            tmdb_keywords: TMDB keywords response object
            job_id: Optional job ID for logging

        Returns:
            List of internal keyword IDs
        """
        if not tmdb_keywords or not tmdb_keywords.keywords:
            return []

        # Initialize cache
        cache_map = await self.cache.get_map(db)

        keyword_ids = []
        uncached_keywords = []
        seen_tmdb_ids = set()

        # Separate cached and uncached keywords
        for kw in tmdb_keywords.keywords:
            if kw.id in seen_tmdb_ids:
                continue
            seen_tmdb_ids.add(kw.id)

            cached_id = cache_map.get(kw.id)
            if cached_id is not None:
                keyword_ids.append(cached_id)
            else:
                uncached_keywords.append({"tmdb_id": kw.id, "name": kw.name})

        # Process uncached keywords
        if uncached_keywords:
            processed_ids = await self._process_uncached_keywords(
                db, uncached_keywords, job_id
            )
            keyword_ids.extend(processed_ids)

        return keyword_ids

    async def _process_uncached_keywords(
        self,
        db: AsyncSession,
        uncached_keywords: List[Dict[str, Any]],
        job_id: Optional[int] = None,
    ) -> List[int]:
        """Process keywords not found in cache."""
        try:
            # Try batch processing first
            tmdb_to_id_mapping = await keyword.upsert_keywords_batch(
                db, uncached_keywords, commit=False, flush=True
            )

            # Update cache
            self.cache.set_batch(tmdb_to_id_mapping)

            # Return IDs in order
            keyword_ids = []
            for item in uncached_keywords:
                internal_id = tmdb_to_id_mapping.get(item["tmdb_id"])
                if internal_id is not None:
                    keyword_ids.append(internal_id)
                else:
                    logger.error(f"Failed to get ID for keyword {item['tmdb_id']}")

            return keyword_ids

        except Exception as e:
            logger.error(f"Batch keyword processing failed: {e}")
            return await self._process_keywords_individually(
                db, uncached_keywords, job_id
            )

    async def _process_keywords_individually(
        self,
        db: AsyncSession,
        keyword_data: List[Dict[str, Any]],
        job_id: Optional[int] = None,
    ) -> List[int]:
        """Fallback to individual keyword processing."""
        keyword_ids = []

        for item in keyword_data:
            try:
                keyword_obj = await keyword.upsert_keyword(
                    db,
                    keyword_id=item["tmdb_id"],
                    name=item["name"],
                    commit=False,
                    flush=True,
                )

                if keyword_obj.id is not None:
                    self.cache.set(item["tmdb_id"], keyword_obj.id)
                    keyword_ids.append(keyword_obj.id)
                else:
                    logger.error(f"Keyword {item['tmdb_id']} has no ID after flush")

            except Exception as e:
                logger.error(f"Failed to process keyword {item['tmdb_id']}: {e}")

        return keyword_ids


# Global processor instance
keyword_processor = KeywordProcessor()
