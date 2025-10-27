Sagepick Core
=============

Backend service that powers Sagepick’s movie discovery workflows. It exposes a FastAPI HTTP interface, orchestrates recurring TMDB synchronisation jobs with APScheduler, and persists state to PostgreSQL with Redis as a coordination cache.

Features
--------
- FastAPI web API with automatic OpenAPI docs at `/docs` and `/redoc`.
- APScheduler-driven background jobs for movie discovery, movie change tracking, and category refresh.
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
DATABASE_URL=postgresql://user:password@localhost:5432/sagepick
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=<256-bit-secret>
SECRET_ISS=SAGEPICK_APP
TMDB_BEARER_TOKEN=<tmdb-token>
```

The scheduler start delay and TMDB throttling can be tuned via `MOVIE_DISCOVERY_START_DELAY_MINUTES`, `TMDB_MAX_REQUESTS_PER_SECOND`, and related settings. See `app/core/settings.py` for the full list.

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

- Override configuration by editing `.env.test` (used by Compose) or supplying your own env file.

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
