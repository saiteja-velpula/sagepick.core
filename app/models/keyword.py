from typing import Optional, List, TYPE_CHECKING
from sqlmodel import SQLModel, Field, Relationship
from .movie_keyword import MovieKeyword

if TYPE_CHECKING:
    from .movie import Movie


class KeywordBase(SQLModel):
    tmdb_id: int = Field(unique=True, index=True, description="TMDB keyword ID")
    name: str = Field(max_length=200, description="Keyword name")


class Keyword(KeywordBase, table=True):
    __tablename__ = "keywords"
    id: Optional[int] = Field(default=None, primary_key=True)

    # Many-to-many relationship with movies
    movies: List["Movie"] = Relationship(
        back_populates="keywords", link_model=MovieKeyword
    )


class KeywordRead(KeywordBase):
    """Schema for reading a keyword."""

    id: int
