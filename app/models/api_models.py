from typing import List, Generic, TypeVar, Optional
from pydantic import BaseModel, Field
from .movie import MovieBase
from .genre import GenreRead
from .keyword import KeywordRead

T = TypeVar('T')


class PaginationInfo(BaseModel):
    """Pagination metadata."""
    page: int = Field(description="Current page number")
    per_page: int = Field(description="Items per page")
    total_items: int = Field(description="Total number of items")
    total_pages: int = Field(description="Total number of pages")
    has_next: bool = Field(description="Whether there is a next page")
    has_prev: bool = Field(description="Whether there is a previous page")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response."""
    data: List[T] = Field(description="List of items")
    pagination: PaginationInfo = Field(description="Pagination information")


class MovieListItem(BaseModel):
    """Movie item for list responses - only essential fields."""
    id: int
    tmdb_id: int
    title: str
    overview: Optional[str]
    backdrop_path: Optional[str]
    poster_path: Optional[str]
    adult: bool
    popularity: float
    vote_average: float
    release_date: Optional[str]  # Using string for date formatting


class MovieDetailResponse(MovieBase):
    """Movie detail response with all fields plus relationships."""
    id: int
    genres: List[GenreRead] = Field(description="Movie genres")
    keywords: List[KeywordRead] = Field(description="Movie keywords")


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
    genres: List[GenreDict] = Field(description="Movie genres as id-name pairs")
    keywords: List[KeywordDict] = Field(description="Movie keywords as id-name pairs")