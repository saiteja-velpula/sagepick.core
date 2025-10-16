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
        keyword_ids: Optional[List[int]] = None
    ) -> Movie:
        # Check if movie exists by tmdb_id
        existing_movie = await self.get_by_tmdb_id(db, movie_create.tmdb_id)
        
        if existing_movie:
            # Update existing movie
            movie_data = movie_create.model_dump(exclude_unset=True)
            for field, value in movie_data.items():
                setattr(existing_movie, field, value)
            db.add(existing_movie)
            movie = existing_movie
        else:
            # Create new movie
            movie_data = movie_create.model_dump()
            movie = Movie(**movie_data)
            db.add(movie)
        
        await db.commit()
        await db.refresh(movie)
        
        # Handle genres
        if genre_ids is not None:
            await self._upsert_movie_genres(db, movie.id, genre_ids)
        
        # Handle keywords
        if keyword_ids is not None:
            await self._upsert_movie_keywords(db, movie.id, keyword_ids)
        
        return movie
    
    async def _upsert_movie_genres(self, db: AsyncSession, movie_id: int, genre_ids: List[int]):
        # Remove existing genres
        statement = select(MovieGenre).where(MovieGenre.movie_id == movie_id)
        result = await db.execute(statement)
        existing_relations = result.scalars().all()
        
        for relation in existing_relations:
            await db.delete(relation)
        
        # Add new genres
        for genre_id in genre_ids:
            movie_genre = MovieGenre(movie_id=movie_id, genre_id=genre_id)
            db.add(movie_genre)
        
        await db.commit()
    
    async def _upsert_movie_keywords(self, db: AsyncSession, movie_id: int, keyword_ids: List[int]):
        # Remove existing keywords
        statement = select(MovieKeyword).where(MovieKeyword.movie_id == movie_id)
        result = await db.execute(statement)
        existing_relations = result.scalars().all()
        
        for relation in existing_relations:
            await db.delete(relation)
        
        # Add new keywords
        for keyword_id in keyword_ids:
            movie_keyword = MovieKeyword(movie_id=movie_id, keyword_id=keyword_id)
            db.add(movie_keyword)
        
        await db.commit()
    
    async def get_movies_by_tmdb_ids(self, db: AsyncSession, tmdb_ids: List[int]) -> List[Movie]:
        statement = select(Movie).where(Movie.tmdb_id.in_(tmdb_ids))
        result = await db.execute(statement)
        return result.scalars().all()


# Singleton instance
movie = CRUDMovie(Movie)