from typing import List, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import date


# Basic data models
class Genre(BaseModel):
    id: int
    name: str


class ProductionCompany(BaseModel):
    id: int
    name: str
    logo_path: Optional[str] = None
    origin_country: str


class ProductionCountry(BaseModel):
    iso_3166_1: str = Field(alias="iso_3166_1")
    name: str

    class Config:
        populate_by_name = True


class SpokenLanguage(BaseModel):
    iso_639_1: str = Field(alias="iso_639_1")
    name: str
    english_name: str

    class Config:
        populate_by_name = True


class Keyword(BaseModel):
    id: int
    name: str


# Main movie models
class MovieItem(BaseModel):
    tmdb_id: int = Field(alias="id")
    title: str
    original_title: str
    overview: Optional[str] = None  # CAN be null/empty
    poster_path: Optional[str] = None  # CAN be null
    backdrop_path: Optional[str] = None  # CAN be null
    release_date: Optional[date] = None
    original_language: str
    vote_average: float
    vote_count: int
    popularity: float
    adult: bool
    genre_ids: List[int] = Field(default_factory=list)

    @field_validator('release_date', pre=True)
    def parse_release_date(cls, v):
        """Handle empty string or null release dates."""
        if not v or v == "":
            return None
        return v

    @field_validator('overview', pre=True)
    def parse_overview(cls, v):
        if v == "":
            return None
        return v

    class Config:
        populate_by_name = True


class MovieDetails(BaseModel):
    tmdb_id: int = Field(alias="id")
    title: str
    original_title: str
    overview: Optional[str] = None  # CAN be null/empty
    poster_path: Optional[str] = None  # CAN be null
    backdrop_path: Optional[str] = None  # CAN be null
    release_date: Optional[date] = None
    original_language: str
    vote_average: float
    vote_count: int
    popularity: float
    adult: bool
    
    # Additional fields only in detailed response
    runtime: Optional[int] = None  # Minutes, can be null/0
    revenue: Optional[int] = None  # Box office, can be 0/null
    budget: Optional[int] = None  # Production cost, can be 0/null
    status: Optional[str] = None  # "Released", "In Production", etc.
    tagline: Optional[str] = None
    homepage: Optional[str] = None
    imdb_id: Optional[str] = None
    
    # Related objects (full data instead of just IDs)
    genres: List[Genre] = Field(default_factory=list)
    production_companies: List[ProductionCompany] = Field(default_factory=list)
    production_countries: List[ProductionCountry] = Field(default_factory=list)
    spoken_languages: List[SpokenLanguage] = Field(default_factory=list)

    @field_validator('release_date', pre=True)
    def parse_release_date(cls, v):
        if not v or v == "":
            return None
        return v

    @field_validator('overview', pre=True)
    def parse_overview(cls, v):
        if v == "":
            return None
        return v

    class Config:
        populate_by_name = True


# API response wrappers
class KeywordsResponse(BaseModel):
    id: int
    keywords: List[Keyword]


class GenresResponse(BaseModel):
    genres: List[Genre]


# Raw TMDB response models (for internal parsing)
class TMDBMovieListResponse(BaseModel):
    page: int
    total_pages: int
    total_results: int
    results: List[MovieItem]


class PaginationInfo(BaseModel):
    page: int
    total_pages: int
    total_results: int


class MovieListResponse(BaseModel):
    movies: List[MovieItem]
    pagination: PaginationInfo


# Search parameters
class MovieSearchParams(BaseModel):
    # Basic search
    query: Optional[str] = None
    page: int = 1
    
    # Date filters
    primary_release_year: Optional[int] = None
    year: Optional[int] = None
    
    # Rating filters
    vote_average_gte: Optional[float] = None
    vote_count_gte: Optional[int] = None
    
    # Genre filters (comma-separated IDs)
    with_genres: Optional[str] = None
    without_genres: Optional[str] = None
    
    # Runtime filters
    with_runtime_gte: Optional[int] = None
    with_runtime_lte: Optional[int] = None
    
    # Origin filters
    with_origin_country: Optional[str] = None
    with_original_language: Optional[str] = None
    
    # Company/keyword filters
    with_companies: Optional[str] = None
    with_keywords: Optional[str] = None
    without_keywords: Optional[str] = None
    
    # Sorting
    sort_by: str = "popularity.desc"
    
    # Content filters
    include_adult: bool = False
    include_video: bool = False