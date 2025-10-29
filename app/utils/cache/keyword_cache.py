import asyncio
import logging
from typing import Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.models.keyword import Keyword

logger = logging.getLogger(__name__)


class KeywordCache:
    def __init__(self):
        self._loaded = False
        self._lock = asyncio.Lock()
        self._map: Dict[int, int] = {}  # tmdb_id -> internal_id

    async def get_map(self, db: AsyncSession) -> Dict[int, int]:
        if self._loaded:
            return self._map

        async with self._lock:
            if not self._loaded:
                await self._load_from_db(db)
        return self._map

    async def _load_from_db(self, db: AsyncSession) -> None:
        try:
            result = await db.execute(select(Keyword.tmdb_id, Keyword.id))
            rows = result.all()
            self._map = {
                int(tmdb_id): int(internal_id) for tmdb_id, internal_id in rows
            }
            self._loaded = True
            logger.info(f"Loaded {len(self._map)} keywords from database")
        except Exception as e:
            logger.error(f"Failed to load keywords from database: {e}")
            self._map = {}
            self._loaded = True

    def get(self, tmdb_id: int) -> int | None:
        return self._map.get(tmdb_id)

    def set(self, tmdb_id: int, internal_id: int) -> None:
        if internal_id is None or internal_id <= 0:
            logger.warning(
                f"Invalid keyword ID: tmdb_id={tmdb_id}, internal_id={internal_id}"
            )
            return
        self._map[tmdb_id] = internal_id

    def set_batch(self, mappings: Dict[int, int]) -> None:
        valid_mappings = {
            tmdb_id: internal_id
            for tmdb_id, internal_id in mappings.items()
            if internal_id is not None and internal_id > 0
        }

        if not valid_mappings:
            return

        self._map.update(valid_mappings)
        logger.debug(f"Batch cached {len(valid_mappings)} keyword mappings")

    def clear(self) -> None:
        self._map.clear()
        self._loaded = False


# Global instance
keyword_cache = KeywordCache()
