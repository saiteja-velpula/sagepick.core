from typing import List, Optional
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.media_category import MediaCategory, MediaCategoryUpdate
from app.models.media_category_movie import MediaCategoryMovie


class CRUDMediaCategory(CRUDBase[MediaCategory, MediaCategory, MediaCategoryUpdate]):
    
    async def get_by_name(self, db: AsyncSession, name: str) -> Optional[MediaCategory]:
        statement = select(MediaCategory).where(MediaCategory.name == name)
        result = await db.execute(statement)
        return result.scalars().first()
    
    async def get_all_categories(self, db: AsyncSession) -> List[MediaCategory]:
        statement = select(MediaCategory)
        result = await db.execute(statement)
        return result.scalars().all()
    
    async def update_category_movies(
        self, 
        db: AsyncSession, 
        category_id: int, 
        movie_ids: List[int]
    ) -> None:
        # Remove existing movie associations
        statement = select(MediaCategoryMovie).where(MediaCategoryMovie.media_category_id == category_id)
        result = await db.execute(statement)
        existing_relations = result.scalars().all()
        
        for relation in existing_relations:
            await db.delete(relation)
        
        # Add new movie associations
        for movie_id in movie_ids:
            category_movie = MediaCategoryMovie(
                media_category_id=category_id, 
                movie_id=movie_id
            )
            db.add(category_movie)
        
        await db.commit()
    
    async def get_category_movie_count(self, db: AsyncSession, category_id: int) -> int:
        statement = select(MediaCategoryMovie).where(MediaCategoryMovie.media_category_id == category_id)
        result = await db.execute(statement)
        relations = result.scalars().all()
        return len(relations)


# Singleton instance
media_category = CRUDMediaCategory(MediaCategory)