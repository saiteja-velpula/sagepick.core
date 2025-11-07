from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Column
from sqlmodel import Field, Relationship, SQLModel

from .movie_genre import MovieGenre
from .movie_keyword import MovieKeyword

if TYPE_CHECKING:
    from .genre import Genre
    from .keyword import Keyword


class MovieBase(SQLModel):
    # Basic movie information
    tmdb_id: int = Field(unique=True, index=True, description="TMDB movie ID")
    title: str = Field(max_length=1000, description="Movie title")
    original_title: str = Field(max_length=1000, description="Original movie title")
    overview: str | None = Field(default=None, description="Movie overview/plot")

    # Paths and media
    poster_path: str | None = Field(
        default=None, max_length=200, description="Poster image path"
    )
    backdrop_path: str | None = Field(
        default=None, max_length=200, description="Backdrop image path"
    )

    # Language
    original_language: str = Field(max_length=10, description="Original language code")

    # Dates
    release_date: date | None = Field(default=None, description="Movie release date")

    # Ratings and popularity
    vote_average: float = Field(default=0.0, description="Average vote score")
    vote_count: int = Field(default=0, description="Total vote count")
    popularity: float = Field(default=0.0, description="Movie popularity score")

    # Other essential details
    runtime: int | None = Field(default=None, description="Runtime in minutes")
    budget: int | None = Field(
        default=None,
        description="Budget in USD",
        sa_column=Column(BigInteger, nullable=True, comment="Budget in USD"),
    )
    revenue: int | None = Field(
        default=None,
        description="Revenue in USD",
        sa_column=Column(BigInteger, nullable=True, comment="Revenue in USD"),
    )
    status: str | None = Field(
        default=None, max_length=50, description="Movie status (e.g., Released)"
    )
    adult: bool = Field(default=False, description="Adult content flag")


class Movie(MovieBase, table=True):
    __tablename__ = "movies"

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Record creation timestamp",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Record update timestamp"
    )

    # Many-to-many relationships
    genres: list["Genre"] = Relationship(back_populates="movies", link_model=MovieGenre)
    keywords: list["Keyword"] = Relationship(
        back_populates="movies", link_model=MovieKeyword
    )


class MovieUpdate(SQLModel):
    title: str | None = None
    original_title: str | None = None
    overview: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    original_language: str | None = None
    release_date: date | None = None
    vote_average: float | None = None
    vote_count: int | None = None
    popularity: float | None = None
    runtime: int | None = None
    budget: int | None = None
    revenue: int | None = None
    status: str | None = None
    adult: bool | None = None


class MovieCreate(MovieBase):
    pass


class MovieRead(MovieBase):
    id: int
    created_at: datetime
    updated_at: datetime
