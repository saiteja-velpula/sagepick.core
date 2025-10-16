from typing import Optional
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.genre import Genre


class CRUDGenre(CRUDBase[Genre, Genre, Genre]):
    
    async def get_by_tmdb_id(self, db: AsyncSession, tmdb_id: int) -> Optional[Genre]:
        statement = select(Genre).where(Genre.tmdb_id == tmdb_id)
        result = await db.execute(statement)
        return result.scalars().first()
    
    async def upsert_genre(self, db: AsyncSession, *, genre_id: int, name: str) -> Genre:
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
        
        await db.commit()
        await db.refresh(genre)
        return genre


# Singleton instance
genre = CRUDGenre(Genre)