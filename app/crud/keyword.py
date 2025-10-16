from typing import Optional
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.keyword import Keyword


class CRUDKeyword(CRUDBase[Keyword, Keyword, Keyword]):
    
    async def get_by_tmdb_id(self, db: AsyncSession, tmdb_id: int) -> Optional[Keyword]:
        statement = select(Keyword).where(Keyword.tmdb_id == tmdb_id)
        result = await db.execute(statement)
        return result.scalars().first()
    
    async def upsert_keyword(self, db: AsyncSession, *, keyword_id: int, name: str) -> Keyword:
        # Check if keyword exists by tmdb_id
        existing_keyword = await self.get_by_tmdb_id(db, keyword_id)
        
        if existing_keyword:
            # Update existing keyword
            existing_keyword.name = name
            db.add(existing_keyword)
            keyword = existing_keyword
        else:
            # Create new keyword
            keyword = Keyword(tmdb_id=keyword_id, name=name)
            db.add(keyword)
        
        await db.commit()
        await db.refresh(keyword)
        return keyword


# Singleton instance
keyword = CRUDKeyword(Keyword)