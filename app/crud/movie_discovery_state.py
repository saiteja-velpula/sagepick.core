from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.models.movie_discovery_state import MovieDiscoveryState


class MovieDiscoveryStateCRUD:
    """CRUD helper for persisting the movie discovery pagination state."""

    async def get_state(self, db: AsyncSession) -> Optional[MovieDiscoveryState]:
        result = await db.execute(select(MovieDiscoveryState).limit(1))
        return result.scalars().first()

    async def get_current_page(self, db: AsyncSession) -> int:
        state = await self.get_state(db)
        if state and state.current_page:
            return state.current_page
        return 1

    async def update_current_page(self, db: AsyncSession, current_page: int) -> MovieDiscoveryState:
        current_page = max(1, current_page)
        state = await self.get_state(db)
        if state:
            state.current_page = current_page
            state.updated_at = datetime.utcnow()
        else:
            state = MovieDiscoveryState(id=1, current_page=current_page, updated_at=datetime.utcnow())
        db.add(state)
        await db.commit()
        await db.refresh(state)
        return state


movie_discovery_state = MovieDiscoveryStateCRUD()
