from pydantic import BaseModel, Field

from .genre import GenreRead
from .keyword import KeywordRead
from .movie import MovieBase


class MovieListItem(BaseModel):
    """Movie item for list responses - only essential fields."""

    id: int
    tmdb_id: int
    title: str
    overview: str | None
    backdrop_path: str | None
    poster_path: str | None
    adult: bool
    popularity: float
    vote_average: float
    release_date: str | None  # Using string for date formatting


class MovieDetailResponse(MovieBase):
    """Movie detail response with all fields plus relationships."""

    id: int
    genres: list[GenreRead] = Field(description="Movie genres")
    keywords: list[KeywordRead] = Field(description="Movie keywords")


class GenreDict(BaseModel):
    """Genre in dictionary format."""

    id: int
    name: str


class KeywordDict(BaseModel):
    """Keyword in dictionary format."""

    id: int
    name: str


class MovieFullDetail(MovieBase):
    """Movie with full details and relationships as dictionaries."""

    id: int
    genres: list[GenreDict] = Field(description="Movie genres as id-name pairs")
    keywords: list[KeywordDict] = Field(description="Movie keywords as id-name pairs")
