from sqlmodel import Field, SQLModel


class MovieKeyword(SQLModel, table=True):
    __tablename__ = "movie_keywords"

    movie_id: int | None = Field(
        default=None, foreign_key="movies.id", primary_key=True
    )
    keyword_id: int | None = Field(
        default=None, foreign_key="keywords.id", primary_key=True
    )
