from typing import Optional, List, Dict, Any
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.crud.base import CRUDBase
from app.models.genre import Genre


class CRUDGenre(CRUDBase[Genre, Genre, Genre]):
    async def get_by_tmdb_id(self, db: AsyncSession, tmdb_id: int) -> Optional[Genre]:
        statement = select(Genre).where(Genre.tmdb_id == tmdb_id)
        result = await db.execute(statement)
        return result.scalars().first()

    async def get_by_tmdb_ids(self, db: AsyncSession, tmdb_ids: List[int]) -> Dict[int, Genre]:
        """Get multiple genres by TMDB IDs in a single query."""
        statement = select(Genre).where(Genre.tmdb_id.in_(tmdb_ids))
        result = await db.execute(statement)
        genres = result.scalars().all()
        return {genre.tmdb_id: genre for genre in genres}

    async def upsert_genre(
        self,
        db: AsyncSession,
        *,
        genre_id: int,
        name: str,
        commit: bool = True,
        flush: bool = True,
    ) -> Genre:
        # Check if genre exists by tmdb_id
        existing_genre = await self.get_by_tmdb_id(db, genre_id)

        if existing_genre:
            # Update existing genre
            existing_genre.name = name
            db.add(existing_genre)
            genre = existing_genre
        else:
            # Create new genre
            genre = Genre(tmdb_id=genre_id, name=name)
            db.add(genre)

        if commit:
            await db.commit()
            await db.refresh(genre)
        elif flush:
            await db.flush()
        return genre

    async def upsert_genres_batch(
        self,
        db: AsyncSession,
    genre_data: List[Dict[str, Any]],  # [{"tmdb_id": 1, "name": "Action"}, ...]
        commit: bool = True,
        flush: bool = True,
    ) -> Dict[int, int]:
        """
        Batch upsert genres and return mapping of tmdb_id -> internal_id.
        More efficient than individual upserts.
        """
        if not genre_data:
            return {}

        tmdb_ids = [item["tmdb_id"] for item in genre_data]

        # Use PostgreSQL native UPSERT with ON CONFLICT DO UPDATE
        stmt = insert(Genre.__table__).values(genre_data)
        
        # ON CONFLICT: when tmdb_id conflicts, only update if name has changed
        stmt = stmt.on_conflict_do_update(
            index_elements=['tmdb_id'],  # The unique index on tmdb_id
            set_={
                'name': stmt.excluded.name
            },
            # Only update when the name is actually different (PostgreSQL-level check)
            where=(Genre.__table__.c.name != stmt.excluded.name)
        )
        
        # Return the IDs using RETURNING clause
        stmt = stmt.returning(Genre.__table__.c.tmdb_id, Genre.__table__.c.id)
        
        # Execute the UPSERT
        result = await db.execute(stmt)
        rows = result.fetchall()
        mapping = {row[0]: row[1] for row in rows}  # tmdb_id -> id

        missing_ids = [tmdb_id for tmdb_id in tmdb_ids if tmdb_id not in mapping]
        if missing_ids:
            existing = await self.get_by_tmdb_ids(db, missing_ids)
            mapping.update({tmdb_id: genre_obj.id for tmdb_id, genre_obj in existing.items()})

        if commit:
            await db.commit()
        elif flush:
            await db.flush()

        return mapping


# Singleton instance
genre = CRUDGenre(Genre)
