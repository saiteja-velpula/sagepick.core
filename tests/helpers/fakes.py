import asyncio


class FakeAsyncContext:
    def __init__(self, result=None):
        self.result = result
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


class FakeAsyncSession:
    def __init__(self, responses=None):
        self.responses = responses or []
        self.flush_calls = 0

    async def execute(self, _):
        return self.responses.pop(0)

    async def flush(self):
        self.flush_calls += 1


class FakeDBResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class DummyTMDBClient:
    def __init__(self, results):
        self.results = results
        self.calls = []

    async def get_movie_by_id(self, movie_id):
        return self.results["details"].pop(0)

    async def get_movie_keywords(self, movie_id):
        return self.results["keywords"].pop(0)


class CancelEvent(asyncio.Event):
    def __init__(self, initial=False):
        super().__init__()
        if initial:
            self.set()
