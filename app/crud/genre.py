from typing import Optional, List, Dict
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

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
        genre_data: List[Dict[str, any]],  # [{"tmdb_id": 1, "name": "Action"}, ...]
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
        
        # Get existing genres in batch
        existing_genres = await self.get_by_tmdb_ids(db, tmdb_ids)
        
        result_mapping = {}
        
        # Process each genre
        for item in genre_data:
            tmdb_id = item["tmdb_id"]
            name = item["name"]
            
            if tmdb_id in existing_genres:
                # Update existing
                existing_genre = existing_genres[tmdb_id]
                existing_genre.name = name
                db.add(existing_genre)
                # Note: ID will be available after flush
            else:
                # Create new
                new_genre = Genre(tmdb_id=tmdb_id, name=name)
                db.add(new_genre)

        if commit:
            await db.commit()
            # Re-fetch to get all IDs
            final_genres = await self.get_by_tmdb_ids(db, tmdb_ids)
            result_mapping = {tmdb_id: genre.id for tmdb_id, genre in final_genres.items()}
        elif flush:
            await db.flush()
            # Get IDs after flush
            final_genres = await self.get_by_tmdb_ids(db, tmdb_ids)
            result_mapping = {tmdb_id: genre.id for tmdb_id, genre in final_genres.items()}

        return result_mapping


# Singleton instance
genre = CRUDGenre(Genre)
