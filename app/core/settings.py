from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class JobSettings(BaseModel):
    movie_items_per_run: int = 20
    movie_items_per_category: int = 20
    tracking_items_per_page: int = 100
    error_rate_threshold: float = 0.9


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

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


# Singleton instance
settings = Settings()
