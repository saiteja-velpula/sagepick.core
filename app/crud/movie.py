from typing import List, Optional
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.movie import Movie, MovieCreate, MovieUpdate
from app.models.movie_genre import MovieGenre
from app.models.movie_keyword import MovieKeyword


class CRUDMovie(CRUDBase[Movie, MovieCreate, MovieUpdate]):
    async def get_by_tmdb_id(self, db: AsyncSession, tmdb_id: int) -> Optional[Movie]:
        statement = select(Movie).where(Movie.tmdb_id == tmdb_id)
        result = await db.execute(statement)
        return result.scalars().first()

    async def upsert_movie_with_relationships(
        self,
        db: AsyncSession,
        *,
        movie_create: MovieCreate,
        genre_ids: Optional[List[int]] = None,
        keyword_ids: Optional[List[int]] = None,
        commit: bool = True,
    ) -> Movie:
        # Check if movie exists by tmdb_id
        existing_movie = await self.get_by_tmdb_id(db, movie_create.tmdb_id)

        movie_updated = False

        if existing_movie:
            # Update existing movie
            movie_data = movie_create.model_dump(exclude_unset=True)
            for field, value in movie_data.items():
                if getattr(existing_movie, field) != value:
                    setattr(existing_movie, field, value)
                    movie_updated = True
            db.add(existing_movie)
            movie = existing_movie
        else:
            # Create new movie
            movie_data = movie_create.model_dump()
            movie = Movie(**movie_data)
            db.add(movie)
            movie_updated = True

        if movie.id is None:
            await db.flush()

        # Handle genres
        relationships_changed = False
        if genre_ids is not None:
            relationships_changed |= await self._upsert_movie_genres(
                db, movie.id, genre_ids, commit=False
            )

        # Handle keywords
        if keyword_ids is not None:
            relationships_changed |= await self._upsert_movie_keywords(
                db, movie.id, keyword_ids, commit=False
            )

        if commit:
            await db.commit()
            await db.refresh(movie)
        elif movie_updated or relationships_changed:
            await db.flush()
        return movie

    async def _upsert_movie_genres(
        self,
        db: AsyncSession,
        movie_id: int,
        genre_ids: List[int],
        *,
        commit: bool = True,
    ) -> bool:
        # Load existing relations once
        statement = select(MovieGenre).where(MovieGenre.movie_id == movie_id)
        result = await db.execute(statement)
        existing_relations = result.scalars().all()

        existing_by_genre = {
            relation.genre_id: relation for relation in existing_relations
        }

        # Normalise desired genres (preserve order, skip duplicates/None)
        ordered_genre_ids: List[int] = []
        seen: set[int] = set()
        for genre_id in genre_ids:
            if genre_id is None or genre_id in seen:
                continue
            seen.add(genre_id)
            ordered_genre_ids.append(genre_id)

        desired_set = set(ordered_genre_ids)
        changed = False

        # Remove stale relations
        for relation in existing_relations:
            if relation.genre_id not in desired_set:
                await db.delete(relation)
                changed = True

        # Add missing relations
        for genre_id in ordered_genre_ids:
            if genre_id not in existing_by_genre:
                db.add(MovieGenre(movie_id=movie_id, genre_id=genre_id))
                changed = True

        if commit:
            await db.commit()
        elif changed:
            await db.flush()

        return changed

    async def _upsert_movie_keywords(
        self,
        db: AsyncSession,
        movie_id: int,
        keyword_ids: List[int],
        *,
        commit: bool = True,
    ) -> bool:
        # Load existing relations once
        statement = select(MovieKeyword).where(MovieKeyword.movie_id == movie_id)
        result = await db.execute(statement)
        existing_relations = result.scalars().all()

        existing_by_keyword = {
            relation.keyword_id: relation for relation in existing_relations
        }

        ordered_keyword_ids: List[int] = []
        seen: set[int] = set()
        for keyword_id in keyword_ids:
            if keyword_id is None or keyword_id in seen:
                continue
            seen.add(keyword_id)
            ordered_keyword_ids.append(keyword_id)

        desired_set = set(ordered_keyword_ids)
        changed = False

        for relation in existing_relations:
            if relation.keyword_id not in desired_set:
                await db.delete(relation)
                changed = True

        for keyword_id in ordered_keyword_ids:
            if keyword_id not in existing_by_keyword:
                db.add(MovieKeyword(movie_id=movie_id, keyword_id=keyword_id))
                changed = True

        if commit:
            await db.commit()
        elif changed:
            await db.flush()

        return changed

    async def get_movies_by_tmdb_ids(
        self, db: AsyncSession, tmdb_ids: List[int]
    ) -> List[Movie]:
        statement = select(Movie).where(Movie.tmdb_id.in_(tmdb_ids))
        result = await db.execute(statement)
        return result.scalars().all()


# Singleton instance
movie = CRUDMovie(Movie)
