from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class JobSettings(BaseModel):
    movie_items_per_run: int = 20
    tracking_items_per_page: int = 100
    error_rate_threshold: float = 0.9

    # Scheduler intervals
    movie_discovery_interval_minutes: int = 2
    change_tracking_hour: int = 2  # Daily at 2:00 AM UTC
    change_tracking_minute: int = 0


class DatasetExportSettings(BaseModel):
    enabled: bool = False
    bucket: str | None = "sagepick-datasets"
    prefix: str = "datasets/movie_items"
    file_name: str = "movie_items.csv"
    endpoint_url: str | None = "https://storage.sagepick.in"
    access_key: str | None = None
    secret_key: str | None = None
    region_name: str | None = "ap-south-1"
    use_ssl: bool = True
    schedule_day_of_week: str = "sat"
    schedule_hour: int = 2
    schedule_minute: int = 0

    @field_validator("schedule_day_of_week")
    @classmethod
    def validate_schedule_day(cls, value: str) -> str:
        if value == "*":
            return value
        allowed = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
        if value.lower() not in allowed:
            raise ValueError("schedule_day_of_week must be one of mon-sun or '*'")
        return value.lower()


class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str
    SECRET_ISS: str
    TMDB_BEARER_TOKEN: str
    REDIS_URL: str
    MOVIE_DISCOVERY_START_DELAY_MINUTES: int = 10
    TMDB_MAX_REQUESTS_PER_SECOND: int = 15
    TMDB_KEYWORD_CACHE_MAX_ENTRIES: int = 500_000
    JOBS: JobSettings = JobSettings()
    DATASET_EXPORT: DatasetExportSettings = DatasetExportSettings()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    @field_validator("DATABASE_URL")
    def validate_database_url(cls, v):
        if not v or not v.startswith(("postgresql://", "postgres://")):
            raise ValueError(
                "DATABASE_URL must be a valid PostgreSQL connection string"
            )
        return v

    @field_validator("REDIS_URL")
    def validate_redis_url(cls, v):
        if not v or not v.startswith("redis://"):
            raise ValueError("REDIS_URL must be a valid Redis connection string")
        return v


# Singleton instance
settings = Settings()
