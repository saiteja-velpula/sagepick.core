import pytest

from app.crud.movie import movie as movie_crud
from app.models.movie_genre import MovieGenre
from app.models.movie_keyword import MovieKeyword


class _EmptyScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _EmptyResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _EmptyScalarResult(self._rows)


class DummySession:
    def __init__(self):
        self.added = []
        self.deleted = []
        self.flushed = False

    async def execute(self, statement):
        return _EmptyResult([])

    async def delete(self, obj):
        self.deleted.append(obj)

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        self.flushed = True

    async def commit(self):
        return None

    async def refresh(self, obj):
        return None


@pytest.mark.asyncio
async def test_upsert_movie_genres_deduplicates_relations():
    session = DummySession()

    changed = await movie_crud._upsert_movie_genres(
        session,
        movie_id=42,
        genre_ids=[1, 1, 2, 2, 3],
        commit=False,
    )

    pairs = {(mg.movie_id, mg.genre_id) for mg in session.added}

    assert pairs == {(42, 1), (42, 2), (42, 3)}
    assert len(session.added) == 3
    assert session.flushed is True
    assert changed is True


@pytest.mark.asyncio
async def test_upsert_movie_keywords_deduplicates_relations():
    session = DummySession()

    changed = await movie_crud._upsert_movie_keywords(
        session,
        movie_id=99,
        keyword_ids=[5, 5, 6],
        commit=False,
    )

    pairs = {(mk.movie_id, mk.keyword_id) for mk in session.added}

    assert pairs == {(99, 5), (99, 6)}
    assert len(session.added) == 2
    assert session.flushed is True
    assert changed is True


@pytest.mark.asyncio
async def test_upsert_movie_genres_no_changes_skips_flush():
    existing = [MovieGenre(movie_id=7, genre_id=3), MovieGenre(movie_id=7, genre_id=4)]

    class SessionWithExisting(DummySession):
        async def execute(self, statement):
            return _EmptyResult(existing)

    session = SessionWithExisting()

    changed = await movie_crud._upsert_movie_genres(
        session,
        movie_id=7,
        genre_ids=[3, 4, 3],
        commit=False,
    )

    assert changed is False
    assert not session.added
    assert not session.deleted
    assert session.flushed is False


@pytest.mark.asyncio
async def test_upsert_movie_keywords_no_changes_skips_flush():
    existing = [
        MovieKeyword(movie_id=11, keyword_id=8),
        MovieKeyword(movie_id=11, keyword_id=9),
    ]

    class SessionWithExisting(DummySession):
        async def execute(self, statement):
            return _EmptyResult(existing)

    session = SessionWithExisting()

    changed = await movie_crud._upsert_movie_keywords(
        session,
        movie_id=11,
        keyword_ids=[8, 9, 8],
        commit=False,
    )

    assert changed is False
    assert not session.added
    assert not session.deleted
    assert session.flushed is False
