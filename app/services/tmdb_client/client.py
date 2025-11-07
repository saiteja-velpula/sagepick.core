from typing import Any

from app.core import ApiClient, RetryConfig
from app.core.settings import settings

from .models import (
    GenresResponse,
    KeywordsResponse,
    MovieChangeResponse,
    MovieDetails,
    MovieListResponse,
    MovieSearchParams,
    PaginationInfo,
    TMDBMovieListResponse,
)


class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self):
        headers = {
            "Authorization": f"Bearer {settings.TMDB_BEARER_TOKEN}",
            "Accept": "application/json",
        }

        retry_config = RetryConfig(
            attempts=3, backoff_ms=500, retry_on_status={408, 429, 500, 502, 503, 504}
        )

        self.client = ApiClient(
            base_url=self.BASE_URL,
            headers=headers,
            retry_config=retry_config,
            timeout=15.0,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.client.close()

    def _build_params(self, **kwargs) -> dict[str, Any]:
        return {k: v for k, v in kwargs.items() if v is not None}

    def _transform_list_response(
        self, response_data: dict[str, Any]
    ) -> MovieListResponse:
        raw_response = TMDBMovieListResponse(**response_data)

        return MovieListResponse(
            movies=raw_response.results,
            pagination=PaginationInfo(
                page=raw_response.page,
                total_pages=raw_response.total_pages,
                total_results=raw_response.total_results,
            ),
        )

    # CORE MOVIE ENDPOINTS

    async def get_movie_by_id(self, movie_id: int) -> MovieDetails:
        params = self._build_params(language="en-US")
        response = await self.client.get(f"/movie/{movie_id}", params=params)
        return MovieDetails(**response)

    async def get_movie_keywords(self, movie_id: int) -> KeywordsResponse:
        response = await self.client.get(f"/movie/{movie_id}/keywords")
        return KeywordsResponse(**response)

    async def get_movie_genres(self) -> GenresResponse:
        params = self._build_params(language="en-US")
        response = await self.client.get("/genre/movie/list", params=params)
        return GenresResponse(**response)

    # DISCOVERY & TRENDING ENDPOINTS

    async def get_trending_movies_day(self, page: int = 1) -> MovieListResponse:
        """Get movies trending today."""
        params = self._build_params(page=page, language="en-US")
        response = await self.client.get("/trending/movie/day", params=params)
        return self._transform_list_response(response)

    async def get_trending_movies_week(self, page: int = 1) -> MovieListResponse:
        """Get movies trending this week."""
        params = self._build_params(page=page, language="en-US")
        response = await self.client.get("/trending/movie/week", params=params)
        return self._transform_list_response(response)

    async def get_trending_movies(self, page: int = 1) -> MovieListResponse:
        """Get trending movies (default to week)."""
        return await self.get_trending_movies_week(page)

    async def get_popular_movies(self, page: int = 1) -> MovieListResponse:
        params = self._build_params(page=page, language="en-US")
        response = await self.client.get("/movie/popular", params=params)
        return self._transform_list_response(response)

    async def get_top_rated_movies(self, page: int = 1) -> MovieListResponse:
        params = self._build_params(page=page, language="en-US")
        response = await self.client.get("/movie/top_rated", params=params)
        return self._transform_list_response(response)

    async def get_upcoming_movies(self, page: int = 1) -> MovieListResponse:
        params = self._build_params(page=page, language="en-US")
        response = await self.client.get("/movie/upcoming", params=params)
        return self._transform_list_response(response)

    async def get_now_playing_movies(self, page: int = 1) -> MovieListResponse:
        params = self._build_params(page=page, language="en-US")
        response = await self.client.get("/movie/now_playing", params=params)
        return self._transform_list_response(response)

    # DISCOVERY WITH FILTERS

    async def discover_movies(
        self, search_params: MovieSearchParams
    ) -> MovieListResponse:
        params = {k: v for k, v in search_params.dict().items() if v is not None}
        params["language"] = "en-US"

        # Handle rating filters with proper API parameter names
        if search_params.vote_average_gte is not None:
            params["vote_average.gte"] = search_params.vote_average_gte
            del params["vote_average_gte"]
        if search_params.vote_count_gte is not None:
            params["vote_count.gte"] = search_params.vote_count_gte
            del params["vote_count_gte"]

        # Handle runtime filters
        if search_params.with_runtime_gte is not None:
            params["with_runtime.gte"] = search_params.with_runtime_gte
            del params["with_runtime_gte"]
        if search_params.with_runtime_lte is not None:
            params["with_runtime.lte"] = search_params.with_runtime_lte
            del params["with_runtime_lte"]

        response = await self.client.get("/discover/movie", params=params)
        return self._transform_list_response(response)

    async def search_movies(self, query: str, page: int = 1) -> MovieListResponse:
        params = self._build_params(
            query=query, page=page, language="en-US", include_adult=False
        )

        response = await self.client.get("/search/movie", params=params)
        return self._transform_list_response(response)

    # CHANGES ENDPOINT
    async def get_movie_changes(self, page: int = 1) -> MovieChangeResponse:
        params = self._build_params(page=page)
        response = await self.client.get("/movie/changes", params=params)
        return MovieChangeResponse(**response)

    # CONVENIENCE METHODS FOR SPECIFIC REGIONS

    # Indian Cinema
    async def get_bollywood_movies(self, page: int = 1) -> MovieListResponse:
        """Get Bollywood (Hindi) movies."""
        search_params = MovieSearchParams(
            page=page,
            with_origin_country="IN",
            with_original_language="hi",
            sort_by="popularity.desc",
        )
        return await self.discover_movies(search_params)

    async def get_tollywood_movies(self, page: int = 1) -> MovieListResponse:
        """Get Tollywood (Telugu) movies."""
        search_params = MovieSearchParams(
            page=page,
            with_origin_country="IN",
            with_original_language="te",
            sort_by="popularity.desc",
        )
        return await self.discover_movies(search_params)

    async def get_kollywood_movies(self, page: int = 1) -> MovieListResponse:
        """Get Kollywood (Tamil) movies."""
        search_params = MovieSearchParams(
            page=page,
            with_origin_country="IN",
            with_original_language="ta",
            sort_by="popularity.desc",
        )
        return await self.discover_movies(search_params)

    async def get_mollywood_movies(self, page: int = 1) -> MovieListResponse:
        """Get Mollywood (Malayalam) movies."""
        search_params = MovieSearchParams(
            page=page,
            with_origin_country="IN",
            with_original_language="ml",
            sort_by="popularity.desc",
        )
        return await self.discover_movies(search_params)

    async def get_sandalwood_movies(self, page: int = 1) -> MovieListResponse:
        """Get Sandalwood (Kannada) movies."""
        search_params = MovieSearchParams(
            page=page,
            with_origin_country="IN",
            with_original_language="kn",
            sort_by="popularity.desc",
        )
        return await self.discover_movies(search_params)

    # Hollywood Cinema
    async def get_hollywood_movies(self, page: int = 1) -> MovieListResponse:
        """Get Hollywood (US English) movies."""
        search_params = MovieSearchParams(
            page=page,
            with_origin_country="US",
            with_original_language="en",
            sort_by="popularity.desc",
        )
        return await self.discover_movies(search_params)


# Asynchronous create client instance
async def create_tmdb_client():
    return TMDBClient()
