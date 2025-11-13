from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

from .movie_genre import MovieGenre

if TYPE_CHECKING:
    from .movie import Movie


class GenreBase(SQLModel):
    tmdb_id: int = Field(unique=True, index=True, description="TMDB genre ID")
    name: str = Field(max_length=100, description="Genre name")


class Genre(GenreBase, table=True):
    __tablename__ = "genres"
    id: int | None = Field(default=None, primary_key=True)

    # Many-to-many relationship with movies
    movies: list["Movie"] = Relationship(back_populates="genres", link_model=MovieGenre)


class GenreRead(GenreBase):
    id: int
