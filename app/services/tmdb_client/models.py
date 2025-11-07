from datetime import date

from pydantic import BaseModel, Field, field_validator


# Basic data models
class Genre(BaseModel):
    id: int
    name: str


class ProductionCompany(BaseModel):
    id: int
    name: str
    logo_path: str | None = None
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
    overview: str | None = None  # CAN be null/empty
    poster_path: str | None = None  # CAN be null
    backdrop_path: str | None = None  # CAN be null
    release_date: date | None = None
    original_language: str
    vote_average: float
    vote_count: int
    popularity: float
    adult: bool
    genre_ids: list[int] = Field(default_factory=list)

    @field_validator("release_date", mode="before")
    def parse_release_date(cls, v):
        """Handle empty string or null release dates."""
        if not v or v == "":
            return None
        return v

    @field_validator("overview", mode="before")
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
    overview: str | None = None  # CAN be null/empty
    poster_path: str | None = None  # CAN be null
    backdrop_path: str | None = None  # CAN be null
    release_date: date | None = None
    original_language: str
    vote_average: float
    vote_count: int
    popularity: float
    adult: bool

    # Additional fields only in detailed response
    runtime: int | None = None  # Minutes, can be null/0
    revenue: int | None = None  # Box office, can be 0/null
    budget: int | None = None  # Production cost, can be 0/null
    status: str | None = None  # "Released", "In Production", etc.
    tagline: str | None = None
    homepage: str | None = None
    imdb_id: str | None = None

    # Related objects (full data instead of just IDs)
    genres: list[Genre] = Field(default_factory=list)
    production_companies: list[ProductionCompany] = Field(default_factory=list)
    production_countries: list[ProductionCountry] = Field(default_factory=list)
    spoken_languages: list[SpokenLanguage] = Field(default_factory=list)

    @field_validator("release_date", mode="before")
    def parse_release_date(cls, v):
        if not v or v == "":
            return None
        return v

    @field_validator("overview", mode="before")
    def parse_overview(cls, v):
        if v == "":
            return None
        return v

    class Config:
        populate_by_name = True


class MovieChangeItem(BaseModel):
    id: int
    adult: bool | None = False

    @field_validator("adult", mode="before")
    def parse_adult(cls, v):
        if v is None:
            return False
        return bool(v)


# API response wrappers
class KeywordsResponse(BaseModel):
    id: int
    keywords: list[Keyword]


class GenresResponse(BaseModel):
    genres: list[Genre]


# Raw TMDB response models (for internal parsing)
class TMDBMovieListResponse(BaseModel):
    page: int
    total_pages: int
    total_results: int
    results: list[MovieItem]


class PaginationInfo(BaseModel):
    page: int
    total_pages: int
    total_results: int


class MovieListResponse(BaseModel):
    movies: list[MovieItem]
    pagination: PaginationInfo


class MovieChangeResponse(BaseModel):
    results: list[MovieChangeItem]
    page: int
    total_pages: int
    total_results: int


# Search parameters
class MovieSearchParams(BaseModel):
    # Basic search
    query: str | None = None
    page: int = 1

    # Date filters
    primary_release_year: int | None = None
    year: int | None = None

    # Rating filters
    vote_average_gte: float | None = None
    vote_count_gte: int | None = None

    # Genre filters (comma-separated IDs)
    with_genres: str | None = None
    without_genres: str | None = None

    # Runtime filters
    with_runtime_gte: int | None = None
    with_runtime_lte: int | None = None

    # Origin filters
    with_origin_country: str | None = None
    with_original_language: str | None = None

    # Company/keyword filters
    with_companies: str | None = None
    with_keywords: str | None = None
    without_keywords: str | None = None

    # Sorting
    sort_by: str = "popularity.desc"

    # Content filters
    include_adult: bool = False
    include_video: bool = False
