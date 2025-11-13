import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.models.genre import Genre

logger = logging.getLogger(__name__)


class GenreCache:
    def __init__(self):
        self._loaded = False
        self._lock = asyncio.Lock()
        self._map: dict[int, int] = {}  # tmdb_id -> internal_id

    async def get_map(self, db: AsyncSession) -> dict[int, int]:
        if self._loaded:
            return self._map

        async with self._lock:
            if not self._loaded:
                await self._load_from_db(db)
        return self._map

    async def _load_from_db(self, db: AsyncSession) -> None:
        try:
            result = await db.execute(select(Genre.tmdb_id, Genre.id))
            rows = result.all()
            self._map = {int(tmdb_id): int(db_id) for tmdb_id, db_id in rows}
            self._loaded = True
            logger.info(f"Loaded {len(self._map)} genres into cache")
        except Exception as e:
            logger.error(f"Failed to load genre cache: {e}")
            self._map = {}
            self._loaded = True

    def get(self, tmdb_id: int) -> int | None:
        return self._map.get(tmdb_id)

    def set(self, tmdb_id: int, internal_id: int) -> None:
        if internal_id is None or internal_id <= 0:
            logger.warning(
                "Attempted to cache invalid genre ID: tmdb_id=%s, internal_id=%s",
                tmdb_id,
                internal_id,
            )
            return
        self._map[tmdb_id] = internal_id

    def set_batch(self, mappings: dict[int, int]) -> None:
        valid_mappings = {
            tmdb_id: internal_id
            for tmdb_id, internal_id in mappings.items()
            if internal_id is not None and internal_id > 0
        }

        if not valid_mappings:
            return

        self._map.update(valid_mappings)
        logger.debug(f"Cached {len(valid_mappings)} genre mappings")

    def clear(self) -> None:
        self._map.clear()
        self._loaded = False


# Global instance
genre_cache = GenreCache()
