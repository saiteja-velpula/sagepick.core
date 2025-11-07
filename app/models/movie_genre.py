from sqlmodel import Field, SQLModel


class MovieGenre(SQLModel, table=True):
    __tablename__ = "movie_genres"

    movie_id: int | None = Field(
        default=None, foreign_key="movies.id", primary_key=True
    )
    genre_id: int | None = Field(
        default=None, foreign_key="genres.id", primary_key=True
    )
