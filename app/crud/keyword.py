from typing import Optional, List, Dict
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.keyword import Keyword


class CRUDKeyword(CRUDBase[Keyword, Keyword, Keyword]):
    async def get_by_tmdb_id(self, db: AsyncSession, tmdb_id: int) -> Optional[Keyword]:
        statement = select(Keyword).where(Keyword.tmdb_id == tmdb_id)
        result = await db.execute(statement)
        return result.scalars().first()

    async def get_by_tmdb_ids(self, db: AsyncSession, tmdb_ids: List[int]) -> Dict[int, Keyword]:
        """Get multiple keywords by TMDB IDs in a single query."""
        statement = select(Keyword).where(Keyword.tmdb_id.in_(tmdb_ids))
        result = await db.execute(statement)
        keywords = result.scalars().all()
        return {keyword.tmdb_id: keyword for keyword in keywords}

    async def upsert_keyword(
        self,
        db: AsyncSession,
        *,
        keyword_id: int,
        name: str,
        commit: bool = True,
        flush: bool = True,
    ) -> Keyword:
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

        if commit:
            await db.commit()
            await db.refresh(keyword)
        elif flush:
            await db.flush()
        return keyword

    async def upsert_keywords_batch(
        self,
        db: AsyncSession,
        keyword_data: List[Dict[str, any]],  # [{"tmdb_id": 1, "name": "superhero"}, ...]
        commit: bool = True,
        flush: bool = True,
    ) -> Dict[int, int]:
        """
        Batch upsert keywords and return mapping of tmdb_id -> internal_id.
        More efficient than individual upserts.
        """
        if not keyword_data:
            return {}

        tmdb_ids = [item["tmdb_id"] for item in keyword_data]
        
        # Get existing keywords in batch
        existing_keywords = await self.get_by_tmdb_ids(db, tmdb_ids)
        
        result_mapping = {}
        
        # Process each keyword
        for item in keyword_data:
            tmdb_id = item["tmdb_id"]
            name = item["name"]
            
            if tmdb_id in existing_keywords:
                # Update existing
                existing_keyword = existing_keywords[tmdb_id]
                existing_keyword.name = name
                db.add(existing_keyword)
            else:
                # Create new
                new_keyword = Keyword(tmdb_id=tmdb_id, name=name)
                db.add(new_keyword)

        if commit:
            await db.commit()
            # Re-fetch to get all IDs
            final_keywords = await self.get_by_tmdb_ids(db, tmdb_ids)
            result_mapping = {tmdb_id: keyword.id for tmdb_id, keyword in final_keywords.items()}
        elif flush:
            await db.flush()
            # Get IDs after flush
            final_keywords = await self.get_by_tmdb_ids(db, tmdb_ids)
            result_mapping = {tmdb_id: keyword.id for tmdb_id, keyword in final_keywords.items()}

        return result_mapping


# Singleton instance
keyword = CRUDKeyword(Keyword)
