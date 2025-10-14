from typing import Optional
from sqlmodel import SQLModel, Field


class MovieKeyword(SQLModel, table=True):
    __tablename__ = "movie_keywords"
    
    movie_id: Optional[int] = Field(default=None, foreign_key="movies.id", primary_key=True)
    keyword_id: Optional[int] = Field(default=None, foreign_key="keywords.id", primary_key=True)