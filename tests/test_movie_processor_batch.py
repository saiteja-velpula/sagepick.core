"""
DEPRECATED: This test file tests the old process_movie_batch function
which has been removed.
"""

import types

import pytest

from app.utils import movie_processor


@pytest.mark.skip(
    reason="Old processor architecture - needs update for new 3-processor design"
)
@pytest.mark.asyncio
async def test_process_movie_batch_aggregates_status(monkeypatch):
    processed_calls = []

    async def fake_increment_counts(db, job_id, *, processed_delta=0, failed_delta=0):
        processed_calls.append((processed_delta, failed_delta))

    async def fake_log(*args, **kwargs):
        return None

    async def fake_process_tmdb_movie(db, tmdb_client, movie_id, caches, **kwargs):
        if movie_id == 1:
            return types.SimpleNamespace(id=movie_id)
        return None

    async def fake_get_map(*args, **kwargs):
        return {}

    monkeypatch.setattr(
        movie_processor.job_status, "increment_counts", fake_increment_counts
    )
    monkeypatch.setattr(movie_processor.job_log, "log_info", fake_log)
    monkeypatch.setattr(movie_processor.job_log, "log_warning", fake_log)
    monkeypatch.setattr(movie_processor.job_log, "log_error", fake_log)
    # Patch the MovieProcessor.process_movie method instead of process_tmdb_movie
    monkeypatch.setattr(
        "app.utils.movie_processor.movie_processor.process_movie",
        fake_process_tmdb_movie,
    )

    class DummySession:
        async def flush(self):
            return None

    result = await movie_processor.process_movie_batch(
        DummySession(),
        tmdb_client=None,
        movie_ids=[1, 2],
        job_id=123,
        use_locks=False,
        cancel_event=None,
    )

    assert result.attempted == 2
    assert result.succeeded == 1
    assert result.failed == 1
    assert processed_calls == [(1, 1)]
