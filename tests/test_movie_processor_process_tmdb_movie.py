import types

import pytest

from app.utils.movie_processor import MovieProcessor


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

    async def fake_process_genres(db, genres, job_id):
        return [1]

    async def fake_process_keywords(db, keywords, job_id):
        return [2]

    async def fake_upsert_movie(db, movie_create, genre_ids, keyword_ids, commit):
        return types.SimpleNamespace(id=3)

    async def fake_flush():
        return None

    class DummySession:
        async def flush(self):
            return None

        async def commit(self):
            return None

        async def rollback(self):
            return None

    # Create TMDB client mock
    tmdb_client = types.SimpleNamespace(
        get_movie_by_id=fake_movie_by_id,
        get_movie_keywords=fake_movie_keywords,
    )

    # Create MovieProcessor instance
    processor = MovieProcessor()

    # Patch the genre and keyword processors
    monkeypatch.setattr(
        processor.genre_processor, "process_genres", fake_process_genres
    )
    monkeypatch.setattr(
        processor.keyword_processor, "process_keywords", fake_process_keywords
    )

    # Patch the CRUD operations
    from app.crud import movie as movie_crud

    monkeypatch.setattr(
        movie_crud, "upsert_movie_with_relationships", fake_upsert_movie
    )

    # Test the MovieProcessor.process_movie method
    result = await processor.process_movie(
        DummySession(),
        tmdb_client,
        movie_id=1,
        job_id=None,
    )

    assert result.id == 3
