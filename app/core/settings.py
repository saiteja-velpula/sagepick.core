from pydantic import BaseModel, validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class JobSettings(BaseModel):
    movie_items_per_run: int = 20
    movie_items_per_category: int = 20
    tracking_items_per_page: int = 100
    error_rate_threshold: float = 0.9
    
    # Scheduler intervals
    movie_discovery_interval_minutes: int = 2
    change_tracking_hour: int = 2  # Daily at 2:00 AM UTC
    change_tracking_minute: int = 0
    category_refresh_hour: int = 5  # Daily at 5:00 AM UTC
    category_refresh_minute: int = 0


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

    @validator('DATABASE_URL')
    def validate_database_url(cls, v):
        if not v or not v.startswith(('postgresql://', 'postgres://')):
            raise ValueError('DATABASE_URL must be a valid PostgreSQL connection string')
        return v

    @validator('SECRET_KEY')
    def validate_secret_key(cls, v):
        if not v or len(v) < 32:
            raise ValueError('SECRET_KEY must be at least 32 characters long')
        return v

    @validator('TMDB_BEARER_TOKEN')
    def validate_tmdb_token(cls, v):
        if not v or len(v) < 20:
            raise ValueError('TMDB_BEARER_TOKEN must be a valid bearer token')
        return v

    @validator('REDIS_URL')
    def validate_redis_url(cls, v):
        if not v or not v.startswith('redis://'):
            raise ValueError('REDIS_URL must be a valid Redis connection string')
        return v

    @validator('MOVIE_DISCOVERY_START_DELAY_MINUTES')
    def validate_discovery_delay(cls, v):
        if v < 0:
            raise ValueError('MOVIE_DISCOVERY_START_DELAY_MINUTES cannot be negative')
        return v

    @validator('TMDB_MAX_REQUESTS_PER_SECOND')
    def validate_tmdb_rate_limit(cls, v):
        if v <= 0 or v > 100:
            raise ValueError('TMDB_MAX_REQUESTS_PER_SECOND must be between 1 and 100')
        return v


# Singleton instance
settings = Settings()
