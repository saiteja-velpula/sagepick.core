import pytest

from app.crud.movie import movie as movie_crud


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
        self.executed_statements = []
        self.flushed = False

    async def execute(self, statement):
        self.executed_statements.append(statement)
        return _EmptyResult([])

    async def delete(self, obj):
        pass

    def add(self, obj):
        pass

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

    # Check that statements were executed (select existing, then insert new)
    assert len(session.executed_statements) >= 2
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

    # Check that statements were executed (select existing, then insert new)
    assert len(session.executed_statements) >= 2
    assert session.flushed is True
    assert changed is True


@pytest.mark.asyncio
async def test_upsert_movie_genres_no_changes_skips_flush():
    existing_genre_ids = [3, 4]

    class SessionWithExisting(DummySession):
        async def execute(self, statement):
            self.executed_statements.append(statement)
            # Return existing genre_ids for the SELECT query
            return _EmptyResult(existing_genre_ids)

    session = SessionWithExisting()

    changed = await movie_crud._upsert_movie_genres(
        session,
        movie_id=7,
        genre_ids=[3, 4, 3],
        commit=False,
    )

    assert changed is False
    assert session.flushed is False


@pytest.mark.asyncio
async def test_upsert_movie_keywords_no_changes_skips_flush():
    existing_keyword_ids = [8, 9]

    class SessionWithExisting(DummySession):
        async def execute(self, statement):
            self.executed_statements.append(statement)
            # Return existing keyword_ids for the SELECT query
            return _EmptyResult(existing_keyword_ids)

    session = SessionWithExisting()

    changed = await movie_crud._upsert_movie_keywords(
        session,
        movie_id=11,
        keyword_ids=[8, 9, 8],
        commit=False,
    )

    assert changed is False
    assert session.flushed is False
