Sagepick Core
=============

Backend service that powers Sagepick’s movie discovery workflows. It exposes a FastAPI HTTP interface, orchestrates recurring TMDB synchronisation jobs with APScheduler, and persists state to PostgreSQL with Redis as a coordination cache.

Features
--------
- FastAPI web API with automatic OpenAPI docs at `/docs` and `/redoc`.
- APScheduler-driven background jobs for movie discovery, movie change tracking, and category refresh.
- Weekly dataset export job that publishes `movie_items.csv` snapshots to S3-compatible storage.
- PostgreSQL persistence via SQLAlchemy/SQLModel and Alembic migrations.
- Redis-backed distributed locking for movie processing.
- Docker Compose stack for local development (API + Postgres + Redis).

Requirements
------------
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) for dependency and virtualenv management (preferred)
- PostgreSQL 15+ (if running outside Docker)
- Redis 7+

Environment
-----------
Copy `.env.example` to `.env` (or `.env.local`) and provide the required secrets:

```

```
DATABASE_URL=postgresql://user:password@localhost:5432/sagepick  # pragma: allowlist secret
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=<256-bit-secret>
SECRET_ISS=SAGEPICK_APP
```
TMDB_BEARER_TOKEN=<tmdb-token>
# Optional dataset export configuration (MinIO/S3)
DATASET_EXPORT__ENABLED=false
DATASET_EXPORT__BUCKET=sagepick-datasets
DATASET_EXPORT__PREFIX=datasets/movie_items
DATASET_EXPORT__FILE_NAME=movie_items.csv
DATASET_EXPORT__ENDPOINT_URL=https://storage.sagepick.in
DATASET_EXPORT__ACCESS_KEY=<minio-access-key>
DATASET_EXPORT__SECRET_KEY=<minio-secret-key>
DATASET_EXPORT__REGION_NAME=us-east-1
DATASET_EXPORT__USE_SSL=true
DATASET_EXPORT__SCHEDULE_DAY_OF_WEEK=sat
DATASET_EXPORT__SCHEDULE_HOUR=3
DATASET_EXPORT__SCHEDULE_MINUTE=0
DATASET_EXPORT__LATEST_OBJECT_ENABLED=true
DATASET_EXPORT__LATEST_PREFIX=
DATASET_EXPORT__LATEST_FILE_NAME=
```

The scheduler start delay and TMDB throttling can be tuned via `MOVIE_DISCOVERY_START_DELAY_MINUTES`, `TMDB_MAX_REQUESTS_PER_SECOND`, and related settings. See `app/core/settings.py` for the full list.

Dataset Export Job
------------------
When `DATASET_EXPORT__ENABLED=true`, the scheduler uploads a timestamped `movie_items.csv` snapshot every Saturday (UTC) to the configured S3/MinIO bucket. Each row contains:

- `movie_id`, `tmdb_id`, `title`, `original_title`, `overview`, `release_date`, `original_language`
- `runtime_minutes`, `status`, `adult`, `vote_average`, `vote_count`, `popularity`, `budget_usd`, `revenue_usd`
- `genres`, `genre_ids`, `genre_count`, `keywords`, `keyword_ids`, `keyword_count`

Multi-valued `genres` and `keywords` are pipe-delimited (e.g. `Drama|Thriller`).

Each run writes a dated object (e.g. `datasets/movie_items/2025-11-01/movie_items.csv`) **and** refreshes a stable, versioned key (`datasets/movie_items/movie_items.csv`). With bucket versioning enabled, every overwrite of the stable key creates a new object version you can roll back to, while the dated paths remain immutable snapshots.

Local Development (uv)
----------------------
1. Install uv if needed: `curl -LsSf https://astral.sh/uv/install.sh | sh`.
2. Sync dependencies (creates `.venv` automatically):
	```bash
	uv sync
	```
3. Apply database migrations:
	```bash
	uv run alembic upgrade head
	```
4. Run the API with auto reload:
	```bash
	uv run uvicorn app.main:app --reload
	```

APScheduler starts with the FastAPI lifespan event; keep to a single process locally unless you need explicit worker scaling.

Docker Workflow
---------------
- Build and run everything (API + Postgres + Redis):
  ```bash
  docker compose up --build
  ```
  The container entrypoint runs `alembic upgrade head` on start before launching Uvicorn.

- Override configuration by editing `.env.local` (used by Compose) or supplying your own env file.

Testing
-------
- Execute unit and integration tests:
  ```bash
  uv run pytest
  ```

Useful Commands
---------------
- Reset seed data (local dev): `uv run scripts/reset_database.py`
- Seed baseline content: `uv run scripts/seed.py`
- Run Alembic autogenerate: `uv run alembic revision --autogenerate -m "describe change"`

Deployment Notes
----------------
- Production builds are published to GHCR by the `release.yml` GitHub Actions workflow when a GitHub Release is published.
- Containers expect the same environment variables described above; ensure the database is reachable, and only start a single scheduler worker for cron consistency.
