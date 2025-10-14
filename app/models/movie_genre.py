from typing import Optional
from sqlmodel import SQLModel, Field


class MovieGenre(SQLModel, table=True):
    __tablename__ = "movie_genres"
    
    movie_id: Optional[int] = Field(default=None, foreign_key="movies.id", primary_key=True)
    genre_id: Optional[int] = Field(default=None, foreign_key="genres.id", primary_key=True)