import types

import pytest

from app.utils import movie_processor


@pytest.mark.asyncio
async def test_process_movie_success(monkeypatch):
    movie_details = types.SimpleNamespace(
        tmdb_id=1,
        title="Movie",
        original_title="Movie",
        overview="",
        release_date="2024-01-01",
        runtime=120,
        budget=0,
        revenue=0,
        vote_average=7.0,
        vote_count=100,
        popularity=10.0,
        poster_path=None,
        backdrop_path=None,
        adult=False,
        original_language="en",
        status="Released",
        genres=[types.SimpleNamespace(id=101, name="Action")],
    )

    keywords_response = types.SimpleNamespace(
        keywords=[types.SimpleNamespace(id=201, name="Hero")]
    )

    async def fake_movie_by_id(movie_id):
        return movie_details

    async def fake_movie_keywords(movie_id):
        return keywords_response

    async def fake_upsert_genre(db, genre_id, name, commit, flush):
        obj = types.SimpleNamespace(id=1)
        return obj

    async def fake_upsert_keyword(db, keyword_id, name, commit, flush):
        obj = types.SimpleNamespace(id=2)
        return obj

    async def fake_upsert_movie(db, movie_create, genre_ids, keyword_ids, commit):
        return types.SimpleNamespace(id=3)

    class DummySession:
        async def flush(self):
            return None

    # Patch the TMDB client calls
    tmdb_client = types.SimpleNamespace(
        get_movie_by_id=fake_movie_by_id,
        get_movie_keywords=fake_movie_keywords,
    )

    # Patch the CRUD operations
    monkeypatch.setattr(movie_processor.genre, "upsert_genre", fake_upsert_genre)
    monkeypatch.setattr(movie_processor.keyword, "upsert_keyword", fake_upsert_keyword)
    monkeypatch.setattr(
        movie_processor.movie, "upsert_movie_with_relationships", fake_upsert_movie
    )

    # Test the new MovieProcessor.process_movie method
    result = await movie_processor.movie_processor.process_movie(
        DummySession(),
        tmdb_client,
        movie_id=1,
        job_id=None,
    )

    assert result.id == 3
