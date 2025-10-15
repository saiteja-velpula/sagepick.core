from sqlmodel import SQLModel, Field


class MediaCategoryMovie(SQLModel, table=True):
    __tablename__ = "media_category_movies"
    
    media_category_id: int = Field(foreign_key="media_categories.id", primary_key=True)
    movie_id: int = Field(foreign_key="movies.id", primary_key=True)