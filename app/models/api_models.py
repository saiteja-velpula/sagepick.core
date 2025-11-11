from enum import Enum

from pydantic import BaseModel, Field

from .genre import GenreRead
from .keyword import KeywordRead
from .movie import MovieBase


class ReleaseYearRange(str, Enum):
    """Release year range categories for filtering."""

    MODERN = "modern"  # 2020-present
    RECENT = "recent"  # 2010-2019
    CLASSIC = "classic"  # 1990-2009
    RETRO = "retro"  # Before 1990
    ALL = "all"  # No filter


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


class ColdStartPreferences(BaseModel):
    """User preferences for cold-start recommendations."""

    genre_ids: list[int] = Field(
        ...,
        min_length=1,
        description="List of preferred genre IDs (at least 1 required)",
    )
    languages: list[str] = Field(
        ...,
        min_length=1,
        description="List of preferred language codes (e.g., 'en', 'hi', 'te')",
    )
    release_year_ranges: list[ReleaseYearRange] = Field(
        ...,
        min_length=1,
        description="List of preferred release year ranges",
    )
    keywords: list[str] | None = Field(
        default=None, description="Optional list of keyword names for fine-tuning"
    )


class RankedMovieItem(MovieListItem):
    """Movie item with ranking score for cold-start recommendations."""

    rank_score: float = Field(
        description="Calculated ranking score based on preference matching"
    )
