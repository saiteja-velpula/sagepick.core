import os
from datetime import date, datetime

os.environ.setdefault("DATABASE_URL", "secret_url")  # pragma: allowlist secret
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("SECRET_ISS", "sagepick")
os.environ.setdefault("TMDB_BEARER_TOKEN", "token")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from app.jobs.dataset_export import DatasetExportJob
from app.services.storage.dataset_builder import DatasetCSVBuilder
from app.services.storage.dataset_writer import S3DatasetWriter


def test_format_row_with_full_data():
    builder = DatasetCSVBuilder()
    row = {
        "movie_id": 123,
        "tmdb_id": 456,
        "title": "Sample Title",
        "original_title": "Original Title",
        "overview": "Sample overview",
        "release_date": date(2024, 1, 15),
        "original_language": "en",
        "runtime_minutes": 120,
        "status": "Released",
        "adult": False,
        "vote_average": 8.7,
        "vote_count": 5400,
        "popularity": 120.5,
        "budget_usd": 100000000,
        "revenue_usd": 350000000,
        "genres": "Drama|Thriller",
        "genre_ids": "18|53",
        "genre_count": 2,
        "keywords": "cult classic|underground fighting",
        "keyword_ids": "5344|9715",
        "keyword_count": 2,
    }

    formatted = builder._format_row(row)

    assert formatted["release_date"] == "2024-01-15"
    assert formatted["adult"] is False
    assert formatted["runtime_minutes"] == 120
    assert formatted["vote_average"] == 8.7
    assert formatted["genre_count"] == 2
    assert formatted["keywords"] == "cult classic|underground fighting"


def test_build_object_key_uses_prefix_and_date():
    job = DatasetExportJob()
    job.config = job.config.model_copy(deep=True)
    job.config.prefix = "exports/movie_items"
    job.config.file_name = "movie_items.csv"

    timestamp = datetime(2025, 11, 1, 10, 30, 0)
    object_key = job._build_object_key(timestamp)

    assert object_key == "exports/movie_items/2025-11-01/movie_items.csv"


def test_s3_dataset_writer_latest_key_defaults_to_prefix_root():
    writer = S3DatasetWriter(
        bucket="bucket",
        prefix="datasets/movie_items",
        file_name="movie_items.csv",
        endpoint_url=None,
        access_key=None,
        secret_key=None,
        region_name=None,
        use_ssl=True,
    )

    assert writer.latest_key() == "datasets/movie_items/movie_items.csv"
