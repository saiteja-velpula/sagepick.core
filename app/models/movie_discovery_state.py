from datetime import datetime

from sqlmodel import Field, SQLModel


class MovieDiscoveryState(SQLModel, table=True):
    __tablename__ = "movie_discovery_state"

    id: int | None = Field(default=1, primary_key=True)
    current_page: int = Field(
        default=1, ge=1, description="Last processed TMDB discover page"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(),
        description="Timestamp of the most recent update",
    )
