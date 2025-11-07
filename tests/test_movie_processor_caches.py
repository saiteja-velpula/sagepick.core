from types import SimpleNamespace

import pytest

from app.core.redis import redis_client
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
async def test_genre_cache_loads_once(monkeypatch):
    cache = GenreCache()
    rows = [(1, 11), (2, 22)]
    fake_session = _FakeSession(rows)

    data_first = await cache.get_map(fake_session)
    assert data_first == {1: 11, 2: 22}
    assert fake_session.calls == 1

    data_second = await cache.get_map(fake_session)
    assert data_second is data_first
    assert fake_session.calls == 1

    await cache.set(fake_session, 3, 33)
    assert data_second[3] == 33


async def _build_keyword_cache(monkeypatch, initial_map, redis_available=True):
    cache = KeywordCache()

    monkeypatch.setattr(redis_client, "redis", None)

    async def fake_initialize():
        if redis_available:
            redis_client.redis = SimpleNamespace()
        else:
            redis_client.redis = None

    async def fake_hgetall(_):
        return {str(k): str(v) for k, v in initial_map.items()}

    async def fake_hset(key, field, value):
        fake_hset.calls.append((key, field, value))

    fake_hset.calls = []

    monkeypatch.setattr(redis_client, "initialize", fake_initialize)
    monkeypatch.setattr(redis_client, "hgetall", fake_hgetall)
    monkeypatch.setattr(redis_client, "hset", fake_hset)

    await cache.get_map()
    return cache, fake_hset.calls


@pytest.mark.asyncio
async def test_keyword_cache_loads_and_persists(monkeypatch):
    cache, calls = await _build_keyword_cache(monkeypatch, {7: 70})

    assert cache._map == {7: 70}

    await cache.set(8, 80)
    assert cache._map[8] == 80
    assert calls == [(cache._REDIS_HASH_KEY, 8, 80)]


@pytest.mark.asyncio
async def test_keyword_cache_handles_missing_redis(monkeypatch):
    cache, calls = await _build_keyword_cache(
        monkeypatch, {9: 90}, redis_available=False
    )

    assert cache._map == {9: 90}

    await cache.set(10, 100)
    assert cache._map[10] == 100
    assert calls == []
