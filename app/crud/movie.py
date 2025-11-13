from datetime import datetime

from sqlalchemy import delete, func, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.crud.base import CRUDBase
from app.models.movie import Movie, MovieCreate, MovieUpdate
from app.models.movie_genre import MovieGenre
from app.models.movie_keyword import MovieKeyword


class CRUDMovie(CRUDBase[Movie, MovieCreate, MovieUpdate]):
    async def get_by_tmdb_id(self, db: AsyncSession, tmdb_id: int) -> Movie | None:
        statement = select(Movie).where(Movie.tmdb_id == tmdb_id)
        result = await db.execute(statement)
        return result.scalars().first()

    async def upsert_movie_with_relationships(
        self,
        db: AsyncSession,
        *,
        movie_create: MovieCreate,
        genre_ids: list[int] | None = None,
        keyword_ids: list[int] | None = None,
        commit: bool = True,
    ) -> Movie:
        # Prepare movie data for UPSERT
        movie_data = movie_create.model_dump()

        # Use PostgreSQL native UPSERT for the movie record
        stmt = insert(Movie.__table__).values(movie_data)

        excluded = stmt.excluded
        update_columns = {
            column: getattr(excluded, column)
            for column in movie_data
            if column != "tmdb_id"
        }
        update_columns["updated_at"] = func.now()

        change_conditions = [
            Movie.__table__.c[column].is_distinct_from(getattr(excluded, column))
            for column in movie_data
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
        genre_ids: list[int],
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
        ordered_genre_ids: list[int] = []
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
                MovieGenre.genre_id.notin_(ordered_genre_ids),
            )
            await db.execute(stmt)
            removed = True

        # Add new relationships using PostgreSQL UPSERT
        new_genre_ids = [
            genre_id
            for genre_id in ordered_genre_ids
            if genre_id not in existing_genre_ids
        ]
        inserted = False
        if new_genre_ids:
            values = [
                {"movie_id": movie_id, "genre_id": genre_id}
                for genre_id in new_genre_ids
            ]

            stmt = insert(MovieGenre.__table__).values(values)
            stmt = stmt.on_conflict_do_nothing(index_elements=["movie_id", "genre_id"])
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
        keyword_ids: list[int],
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
        ordered_keyword_ids: list[int] = []
        seen: set[int] = set()
        for keyword_id in keyword_ids:
            if keyword_id is None or keyword_id in seen:
                continue
            seen.add(keyword_id)
            ordered_keyword_ids.append(keyword_id)

        # Load existing keyword IDs for this movie
        statement = select(MovieKeyword.keyword_id).where(
            MovieKeyword.movie_id == movie_id
        )
        result = await db.execute(statement)
        existing_keyword_ids = set(result.scalars().all())

        desired_keyword_ids = set(ordered_keyword_ids)

        # Check if any changes are needed
        if existing_keyword_ids == desired_keyword_ids:
            return False  # No changes needed

        removed = False
        if (
            existing_keyword_ids - desired_keyword_ids
        ):  # If there are keywords to remove
            stmt = delete(MovieKeyword).where(
                MovieKeyword.movie_id == movie_id,
                MovieKeyword.keyword_id.notin_(ordered_keyword_ids),
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
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["movie_id", "keyword_id"]
            )
            await db.execute(stmt)
            inserted = True

        if commit:
            await db.commit()
        elif removed or inserted:
            await db.flush()

        return removed or inserted

    async def get_movies_by_tmdb_ids(
        self, db: AsyncSession, tmdb_ids: list[int]
    ) -> list[Movie]:
        statement = select(Movie).where(Movie.tmdb_id.in_(tmdb_ids))
        result = await db.execute(statement)
        return result.scalars().all()

    async def get_by_tmdb_ids(
        self, db: AsyncSession, tmdb_ids: list[int]
    ) -> list[Movie]:
        """Alias for get_movies_by_tmdb_ids for consistency."""
        return await self.get_movies_by_tmdb_ids(db, tmdb_ids)

    async def get_multi_by_ids(self, db: AsyncSession, ids: list[int]) -> list[Movie]:
        """Get multiple movies by their internal IDs."""
        if not ids:
            return []

        statement = select(Movie).where(Movie.id.in_(set(ids)))
        result = await db.execute(statement)
        movies = result.scalars().all()
        movie_map = {movie.id: movie for movie in movies}
        return [movie_map[mid] for mid in ids if mid in movie_map]

    async def insert_movie_from_tmdb_list(
        self,
        db: AsyncSession,
        tmdb_movie_item: "MovieItem",  # type: ignore  # noqa: F821
        *,
        commit: bool = True,
    ) -> Movie:
        """Insert movie with minimal data from TMDB list endpoints.

        This function stores only the data available in TMDB list responses
        (search, discover, trending, etc.) without making additional API calls.
        The movie is marked as not hydrated (is_hydrated=False).

        Args:
            db: Database session
            tmdb_movie_item: MovieItem from TMDB list endpoint response
            commit: Whether to commit the transaction

        Returns:
            Movie object with minimal data (not hydrated)
        """
        # Prepare movie data with defaults for fields not in list response
        movie_data = {
            "tmdb_id": tmdb_movie_item.tmdb_id,
            "title": tmdb_movie_item.title,
            "original_title": tmdb_movie_item.original_title,
            "overview": tmdb_movie_item.overview or "",
            "poster_path": tmdb_movie_item.poster_path,
            "backdrop_path": tmdb_movie_item.backdrop_path,
            "original_language": tmdb_movie_item.original_language,
            "release_date": tmdb_movie_item.release_date,
            "vote_average": tmdb_movie_item.vote_average,
            "vote_count": tmdb_movie_item.vote_count,
            "popularity": tmdb_movie_item.popularity,
            "adult": tmdb_movie_item.adult,
            # Fields not available in list response - use defaults
            "runtime": None,
            "budget": None,
            "revenue": None,
            "status": None,
            # Hydration tracking
            "is_hydrated": False,
            "last_hydrated_at": None,
            "hydration_source": None,
        }

        # Use PostgreSQL INSERT ... ON CONFLICT DO NOTHING
        # We only insert if movie doesn't exist; we don't update existing movies
        stmt = insert(Movie.__table__).values(movie_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=[Movie.__table__.c.tmdb_id])
        stmt = stmt.returning(Movie.__table__.c.id)

        result = await db.execute(stmt)
        row = result.fetchone()

        if commit:
            await db.commit()

        # If row is None, movie already existed - fetch it
        if row is None:
            existing = await self.get_by_tmdb_id(db, tmdb_movie_item.tmdb_id)
            if existing is None:
                raise ValueError(
                    f"Failed to insert movie with tmdb_id={tmdb_movie_item.tmdb_id}"
                )
            return existing

        # Fetch the newly inserted movie
        movie_id = row[0]
        return await self.get(db, movie_id)

    async def insert_movies_from_tmdb_list_batch(
        self,
        db: AsyncSession,
        tmdb_movie_items: list["MovieItem"],  # type: ignore  # noqa: F821
        *,
        commit: bool = True,
    ) -> list[Movie]:
        """Batch insert movies from TMDB list endpoints.

        Args:
            db: Database session
            tmdb_movie_items: List of MovieItem from TMDB list response
            commit: Whether to commit the transaction

        Returns:
            List of Movie objects (newly inserted or existing)
        """
        if not tmdb_movie_items:
            return []

        # Prepare batch insert data
        movies_data = []
        for item in tmdb_movie_items:
            movies_data.append(
                {
                    "tmdb_id": item.tmdb_id,
                    "title": item.title,
                    "original_title": item.original_title,
                    "overview": item.overview or "",
                    "poster_path": item.poster_path,
                    "backdrop_path": item.backdrop_path,
                    "original_language": item.original_language,
                    "release_date": item.release_date,
                    "vote_average": item.vote_average,
                    "vote_count": item.vote_count,
                    "popularity": item.popularity,
                    "adult": item.adult,
                    "runtime": None,
                    "budget": None,
                    "revenue": None,
                    "status": None,
                    "is_hydrated": False,
                    "last_hydrated_at": None,
                    "hydration_source": None,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            )

        # Batch insert with ON CONFLICT DO NOTHING
        stmt = insert(Movie.__table__).values(movies_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=[Movie.__table__.c.tmdb_id])

        await db.execute(stmt)

        if commit:
            await db.commit()

        # Fetch all movies (both newly inserted and existing)
        tmdb_ids = [item.tmdb_id for item in tmdb_movie_items]
        return await self.get_by_tmdb_ids(db, tmdb_ids)


# Singleton instance
movie = CRUDMovie(Movie)
