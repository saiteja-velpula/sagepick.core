from typing import Optional, List, Dict, Any
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

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
        keyword_data: List[Dict[str, Any]],  # [{"tmdb_id": 1, "name": "superhero"}, ...]
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

        # Use PostgreSQL native UPSERT with ON CONFLICT DO UPDATE
        stmt = insert(Keyword.__table__).values(keyword_data)
        
        # ON CONFLICT: when tmdb_id conflicts, only update if name has changed
        stmt = stmt.on_conflict_do_update(
            index_elements=['tmdb_id'],
            set_={
                'name': stmt.excluded.name
            },
            where=(Keyword.__table__.c.name != stmt.excluded.name)
        )
        
        # Return the IDs using RETURNING clause
        stmt = stmt.returning(Keyword.__table__.c.tmdb_id, Keyword.__table__.c.id)
        
        # Execute the UPSERT
        result = await db.execute(stmt)
        rows = result.fetchall()
        mapping = {row[0]: row[1] for row in rows}  # tmdb_id -> id

        missing_ids = [tmdb_id for tmdb_id in tmdb_ids if tmdb_id not in mapping]
        if missing_ids:
            existing = await self.get_by_tmdb_ids(db, missing_ids)
            mapping.update({tmdb_id: keyword_obj.id for tmdb_id, keyword_obj in existing.items()})

        if commit:
            await db.commit()
        elif flush:
            await db.flush()

        return mapping


# Singleton instance
keyword = CRUDKeyword(Keyword)
