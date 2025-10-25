import pytest

from app.services.tmdb_client.client import TMDBClient


@pytest.mark.asyncio
async def test_tmdb_client_transform_list_response(monkeypatch):
    client = TMDBClient()

    async def fake_get(endpoint, params=None):
        return {
            "page": 1,
            "total_pages": 2,
            "total_results": 40,
            "results": [
                {
                    "id": 1,
                    "title": "Test Movie",
                    "overview": "Overview",
                    "vote_average": 7.0,
                    "vote_count": 100,
                    "popularity": 10.0,
                    "poster_path": "/poster.jpg",
                    "backdrop_path": "/backdrop.jpg",
                    "original_title": "Test Movie",
                    "original_language": "en",
                    "genre_ids": [1, 2],
                    "release_date": "2024-01-01",
                    "adult": False,
                    "video": False,
                }
            ],
        }

    monkeypatch.setattr(client.client, "get", fake_get)

    response = await client.get_popular_movies(page=1)
    assert response.pagination.total_results == 40
    assert response.movies[0].title == "Test Movie"

    await client.close()
