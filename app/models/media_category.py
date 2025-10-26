from typing import Optional, List, TYPE_CHECKING
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship
from .media_category_movie import MediaCategoryMovie

if TYPE_CHECKING:
    from .movie import Movie


class MediaCategoryBase(SQLModel):
    name: str = Field(
        max_length=200,
        unique=True,
        index=True,
        description="Category name (e.g., Trending Movies, Popular Movies)",
    )
    description: Optional[str] = Field(
        default=None, description="Description of the media category"
    )


class MediaCategory(MediaCategoryBase, table=True):
    __tablename__ = "media_categories"

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record update timestamp"
    )

    # Many-to-many relationship with movies
    movies: List["Movie"] = Relationship(
        back_populates="media_categories", link_model=MediaCategoryMovie
    )


class MediaCategoryRead(MediaCategoryBase):
    id: int
    created_at: datetime
    updated_at: datetime


class MediaCategoryUpdate(SQLModel):
    name: Optional[str] = None
