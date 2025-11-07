import logging
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.tmdb import get_tmdb_client
from app.crud.genre import genre as genre_crud
from app.utils.cache.genre_cache import genre_cache

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def preload_genres(db: AsyncSession) -> None:
    """Fetch all movie genres from TMDB and store them in the database."""
    try:
        tmdb_client = await get_tmdb_client()
        response = await tmdb_client.get_movie_genres()

        genres = getattr(response, "genres", None)
        if not genres:
            logger.warning("TMDB returned no genres; skipping preload")
            return

        payload: Sequence[dict[str, int | str]] = [
            {"tmdb_id": genre.id, "name": genre.name} for genre in genres
        ]

        mapping = await genre_crud.upsert_genres_batch(
            db, payload, commit=True, flush=False
        )
        logger.info("Preloaded %d genres from TMDB", len(mapping))

        # Warm the in-memory cache with the latest values
        genre_cache.clear()
        await genre_cache.get_map(db)

    except Exception as exc:
        logger.exception("Failed to preload genres from TMDB: %s", exc)
