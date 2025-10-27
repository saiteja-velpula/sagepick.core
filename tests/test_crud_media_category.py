import pytest

from app.crud.media_category import media_category as media_category_crud
from app.models.media_category_movie import MediaCategoryMovie


class _DummyScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _DummyResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _DummyScalarResult(self._rows)


class DummySession:
    def __init__(self, existing=None):
        self._existing = list(existing or [])
        self.deleted = []
        self.added = []
        self.committed = False

    async def execute(self, statement):
        return _DummyResult(self._existing)

    async def delete(self, obj):
        self.deleted.append(obj)

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.committed = True


@pytest.mark.asyncio
async def test_update_category_movies_deduplicates_and_preserves_order():
    existing = [
        MediaCategoryMovie(media_category_id=1, movie_id=5),
        MediaCategoryMovie(media_category_id=1, movie_id=6),
    ]
    session = DummySession(existing=existing)

    await media_category_crud.update_category_movies(
        session,
        category_id=7,
        movie_ids=[10, 10, None, 11, 12, 12, 13],
    )

    assert session.deleted == existing
    added_pairs = [(item.media_category_id, item.movie_id) for item in session.added]
    assert added_pairs == [(7, 10), (7, 11), (7, 12), (7, 13)]
    assert session.committed is True
