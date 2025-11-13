import pytest

from app.utils.cache.genre_cache import GenreCache
from app.utils.cache.keyword_cache import KeywordCache


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    async def execute(self, _):
        self.calls += 1
        return _FakeResult(self._rows)


@pytest.mark.asyncio
async def test_genre_cache_loads_once():
    cache = GenreCache()
    rows = [(1, 11), (2, 22)]
    fake_session = _FakeSession(rows)

    data_first = await cache.get_map(fake_session)
    assert data_first == {1: 11, 2: 22}
    assert fake_session.calls == 1

    data_second = await cache.get_map(fake_session)
    assert data_second is data_first
    assert fake_session.calls == 1

    cache.set(3, 33)
    assert data_second[3] == 33


@pytest.mark.asyncio
async def test_keyword_cache_loads_and_persists():
    cache = KeywordCache()
    rows = [(7, 70)]
    fake_session = _FakeSession(rows)

    data = await cache.get_map(fake_session)
    assert data == {7: 70}
    assert fake_session.calls == 1

    cache.set(8, 80)
    assert cache._map[8] == 80


@pytest.mark.asyncio
async def test_keyword_cache_handles_missing_redis():
    # This test is now about basic functionality since we removed Redis
    cache = KeywordCache()
    rows = [(9, 90)]
    fake_session = _FakeSession(rows)

    data = await cache.get_map(fake_session)
    assert data == {9: 90}

    cache.set(10, 100)
    assert cache._map[10] == 100
