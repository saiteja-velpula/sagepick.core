from typing import Optional, List, TYPE_CHECKING
from datetime import date, datetime
from sqlmodel import SQLModel, Field, Relationship
from .movie_genre import MovieGenre
from .movie_keyword import MovieKeyword
from .media_category_movie import MediaCategoryMovie

if TYPE_CHECKING:
    from .genre import Genre
    from .keyword import Keyword
    from .media_category import MediaCategory


class MovieBase(SQLModel):
    # Basic movie information
    tmdb_id: int = Field(unique=True, index=True, description="TMDB movie ID")
    title: str = Field(max_length=500, description="Movie title")
    original_title: str = Field(max_length=500, description="Original movie title")
    overview: Optional[str] = Field(default=None, description="Movie overview/plot")
    
    # Paths and media
    poster_path: Optional[str] = Field(default=None, max_length=200, description="Poster image path")
    backdrop_path: Optional[str] = Field(default=None, max_length=200, description="Backdrop image path")
    
    # Language
    original_language: str = Field(max_length=10, description="Original language code")
    
    # Dates
    release_date: Optional[date] = Field(default=None, description="Movie release date")
    
    # Ratings and popularity
    vote_average: float = Field(default=0.0, description="Average vote score")
    vote_count: int = Field(default=0, description="Total vote count") 
    popularity: float = Field(default=0.0, description="Movie popularity score")
    
    # Other essential details
    runtime: Optional[int] = Field(default=None, description="Runtime in minutes")
    budget: Optional[int] = Field(default=None, description="Budget in USD")
    revenue: Optional[int] = Field(default=None, description="Revenue in USD")
    status: Optional[str] = Field(default=None, max_length=50, description="Movie status (e.g., Released)")
    adult: bool = Field(default=False, description="Adult content flag")


class Movie(MovieBase, table=True):
    __tablename__ = "movies"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Record creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Record update timestamp")
    
    # Many-to-many relationships
    genres: List["Genre"] = Relationship(back_populates="movies", link_model=MovieGenre)
    keywords: List["Keyword"] = Relationship(back_populates="movies", link_model=MovieKeyword)
    media_categories: List["MediaCategory"] = Relationship(back_populates="movies", link_model=MediaCategoryMovie)


class MovieUpdate(SQLModel):
    title: Optional[str] = None
    original_title: Optional[str] = None
    overview: Optional[str] = None
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None
    original_language: Optional[str] = None
    release_date: Optional[date] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    popularity: Optional[float] = None
    runtime: Optional[int] = None
    adult: Optional[bool] = None


class MovieRead(MovieBase):
    id: int
    created_at: datetime
    updated_at: datetime