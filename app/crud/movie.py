from typing import List, Optional
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import delete, func, or_

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
        # Prepare movie data for UPSERT
        movie_data = movie_create.model_dump()

        # Use PostgreSQL native UPSERT for the movie record
        stmt = insert(Movie.__table__).values(movie_data)

        excluded = stmt.excluded
        update_columns = {
            column: getattr(excluded, column)
            for column in movie_data.keys()
            if column != "tmdb_id"
        }
        update_columns["updated_at"] = func.now()

        change_conditions = [
            Movie.__table__.c[column].is_distinct_from(getattr(excluded, column))
            for column in movie_data.keys()
            if column != "tmdb_id"
        ]

        on_conflict_kwargs = {
            "index_elements": [Movie.__table__.c.tmdb_id],
            "set_": update_columns,
        }

        if change_conditions:
            on_conflict_kwargs["where"] = (
                or_(*change_conditions)
                if len(change_conditions) > 1
                else change_conditions[0]
            )

        stmt = stmt.on_conflict_do_update(**on_conflict_kwargs).returning(
            Movie.__table__.c.id
        )

        result = await db.execute(stmt)
        row = result.fetchone()
        movie_id = row[0] if row else None

        if movie_id is None:
            existing = await self.get_by_tmdb_id(db, movie_create.tmdb_id)
            if existing is None:
                raise ValueError(
                    f"Failed to upsert movie with tmdb_id={movie_create.tmdb_id}"
                )
            movie_id = existing.id
        
        # Handle relationships with the movie ID
        relationships_changed = False
        if genre_ids is not None:
            relationships_changed |= await self._upsert_movie_genres(
                db, movie_id, genre_ids, commit=False
            )

        if keyword_ids is not None:
            relationships_changed |= await self._upsert_movie_keywords(
                db, movie_id, keyword_ids, commit=False
            )

        if commit:
            await db.commit()
        elif relationships_changed:
            await db.flush()

        return await self.get(db, movie_id)

    async def _upsert_movie_genres(
        self,
        db: AsyncSession,
        movie_id: int,
        genre_ids: List[int],
        *,
        commit: bool = True,
    ) -> bool:
        if not genre_ids:
            # If no genres provided, remove all existing relationships
            stmt = delete(MovieGenre).where(MovieGenre.movie_id == movie_id)
            await db.execute(stmt)
            if commit:
                await db.commit()
            return True

        # Normalize desired genres (preserve order, skip duplicates/None)
        ordered_genre_ids: List[int] = []
        seen: set[int] = set()
        for genre_id in genre_ids:
            if genre_id is None or genre_id in seen:
                continue
            seen.add(genre_id)
            ordered_genre_ids.append(genre_id)

        # Load existing genre IDs for this movie
        statement = select(MovieGenre.genre_id).where(MovieGenre.movie_id == movie_id)
        result = await db.execute(statement)
        existing_genre_ids = set(result.scalars().all())
        
        desired_genre_ids = set(ordered_genre_ids)
        
        # Check if any changes are needed
        if existing_genre_ids == desired_genre_ids:
            return False  # No changes needed
        
        # Remove stale relationships (PostgreSQL DELETE WHERE NOT IN)
        removed = False
        if existing_genre_ids - desired_genre_ids:  # If there are genres to remove
            stmt = delete(MovieGenre).where(
                MovieGenre.movie_id == movie_id,
                MovieGenre.genre_id.notin_(ordered_genre_ids)
            )
            await db.execute(stmt)
            removed = True

        # Add new relationships using PostgreSQL UPSERT
        new_genre_ids = [
            genre_id for genre_id in ordered_genre_ids if genre_id not in existing_genre_ids
        ]
        inserted = False
        if new_genre_ids:
            values = [
                {"movie_id": movie_id, "genre_id": genre_id}
                for genre_id in new_genre_ids
            ]

            stmt = insert(MovieGenre.__table__).values(values)
            stmt = stmt.on_conflict_do_nothing(index_elements=['movie_id', 'genre_id'])
            await db.execute(stmt)
            inserted = True

        if commit:
            await db.commit()
        elif removed or inserted:
            await db.flush()

        return removed or inserted

    async def _upsert_movie_keywords(
        self,
        db: AsyncSession,
        movie_id: int,
        keyword_ids: List[int],
        *,
        commit: bool = True,
    ) -> bool:
        if not keyword_ids:
            # If no keywords provided, remove all existing relationships
            stmt = delete(MovieKeyword).where(MovieKeyword.movie_id == movie_id)
            await db.execute(stmt)
            if commit:
                await db.commit()
            return True

        # Normalize desired keywords (preserve order, skip duplicates/None)
        ordered_keyword_ids: List[int] = []
        seen: set[int] = set()
        for keyword_id in keyword_ids:
            if keyword_id is None or keyword_id in seen:
                continue
            seen.add(keyword_id)
            ordered_keyword_ids.append(keyword_id)

        # Load existing keyword IDs for this movie
        statement = select(MovieKeyword.keyword_id).where(MovieKeyword.movie_id == movie_id)
        result = await db.execute(statement)
        existing_keyword_ids = set(result.scalars().all())
        
        desired_keyword_ids = set(ordered_keyword_ids)
        
        # Check if any changes are needed
        if existing_keyword_ids == desired_keyword_ids:
            return False  # No changes needed
        
        removed = False
        if existing_keyword_ids - desired_keyword_ids:  # If there are keywords to remove
            stmt = delete(MovieKeyword).where(
                MovieKeyword.movie_id == movie_id,
                MovieKeyword.keyword_id.notin_(ordered_keyword_ids)
            )
            await db.execute(stmt)
            removed = True

        # Add new relationships using PostgreSQL UPSERT
        new_keyword_ids = [
            keyword_id
            for keyword_id in ordered_keyword_ids
            if keyword_id not in existing_keyword_ids
        ]
        inserted = False
        if new_keyword_ids:  # If there are new keywords to add
            values = [
                {"movie_id": movie_id, "keyword_id": keyword_id}
                for keyword_id in new_keyword_ids
            ]

            stmt = insert(MovieKeyword.__table__).values(values)
            stmt = stmt.on_conflict_do_nothing(index_elements=['movie_id', 'keyword_id'])
            await db.execute(stmt)
            inserted = True

        if commit:
            await db.commit()
        elif removed or inserted:
            await db.flush()

        return removed or inserted

    async def get_movies_by_tmdb_ids(
        self, db: AsyncSession, tmdb_ids: List[int]
    ) -> List[Movie]:
        statement = select(Movie).where(Movie.tmdb_id.in_(tmdb_ids))
        result = await db.execute(statement)
        return result.scalars().all()

    async def get_by_tmdb_ids(
        self, db: AsyncSession, tmdb_ids: List[int]
    ) -> List[Movie]:
        """Alias for get_movies_by_tmdb_ids for consistency."""
        return await self.get_movies_by_tmdb_ids(db, tmdb_ids)

    async def get_multi_by_ids(
        self, db: AsyncSession, ids: List[int]
    ) -> List[Movie]:
        """Get multiple movies by their internal IDs."""
        if not ids:
            return []

        statement = select(Movie).where(Movie.id.in_(set(ids)))
        result = await db.execute(statement)
        movies = result.scalars().all()
        movie_map = {movie.id: movie for movie in movies}
        return [movie_map[mid] for mid in ids if mid in movie_map]


# Singleton instance
movie = CRUDMovie(Movie)
