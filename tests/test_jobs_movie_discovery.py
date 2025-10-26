import asyncio
import types

import pytest


@pytest.mark.asyncio
async def test_movie_discovery_job_handles_empty(monkeypatch):
    from app.jobs.movie_discovery import MovieDiscoveryJob

    job = MovieDiscoveryJob()

    async def fake_get_session():
        async def _gen():
            yield types.SimpleNamespace()

        async for session in _gen():
            yield session

    async def fake_create_job(db, job_type, total_items):
        return types.SimpleNamespace(id=1)

    async def fake_register(job_id, job_type):
        return asyncio.Event()

    async def fake_start_job(db, job_id):
        return None

    async def fake_log(*args, **kwargs):
        return None

    async def fake_get_current_page(db):
        return 1

    class FakeTMDB:
        async def discover_movies(self, search_params):
            return types.SimpleNamespace(
                movies=[], pagination=types.SimpleNamespace(total_pages=1)
            )

        async def close(self):
            return None

    async def fake_process_movie_batch(*args, **kwargs):
        return types.SimpleNamespace(
            attempted=0, succeeded=0, failed=0, skipped_locked=0
        )

    monkeypatch.setattr("app.core.db.get_session", fake_get_session)
    monkeypatch.setattr("app.crud.job_status.create_job", fake_create_job)
    monkeypatch.setattr(
        "app.core.job_execution.job_execution_manager.register", fake_register
    )
    monkeypatch.setattr("app.crud.job_status.start_job", fake_start_job)
    monkeypatch.setattr("app.crud.job_log.log_info", fake_log)
    monkeypatch.setattr("app.crud.job_log.log_warning", fake_log)
    monkeypatch.setattr("app.crud.job_log.log_error", fake_log)
    monkeypatch.setattr(
        "app.crud.movie_discovery_state.get_current_page", fake_get_current_page
    )
    monkeypatch.setattr(
        "app.utils.movie_processor.process_movie_batch", fake_process_movie_batch
    )
    monkeypatch.setattr("app.jobs.movie_discovery.TMDBClient", lambda: FakeTMDB())

    await job.run()
