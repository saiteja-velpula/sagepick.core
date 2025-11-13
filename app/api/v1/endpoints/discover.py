from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func, select

from app.api.deps import verify_token
from app.core.db import get_session
from app.core.tmdb import get_tmdb_client
from app.crud.movie import movie as movie_crud
from app.models.api_models import GenreDict, MovieListItem
from app.models.genre import Genre
from app.models.movie import Movie
from app.models.movie_genre import MovieGenre
from app.services.tmdb_client.models import MovieSearchParams
from app.utils.movie_processor import insert_from_list_and_queue
from app.utils.pagination import (
    PaginatedResponse,
    calculate_offset,
    create_pagination_info,
)

TMDB_PAGE_SIZE = 20

router = APIRouter()


@router.get("/genres", response_model=list[GenreDict])
async def get_genres(
    db: AsyncSession = Depends(get_session), token: dict = Depends(verify_token)
):
    """Get all available movie genres."""
    result = await db.execute(select(Genre))
    genres = result.scalars().all()

    return [GenreDict(id=genre.id, name=genre.name) for genre in genres]


@router.get("/discover", response_model=PaginatedResponse[MovieListItem])
async def discover_movies(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    # Filtering parameters
    with_genres: str | None = Query(None, description="Genre IDs (comma-separated)"),
    without_genres: str | None = Query(
        None, description="Exclude genre IDs (comma-separated)"
    ),
    with_keywords: str | None = Query(
        None, description="Keyword IDs (comma-separated)"
    ),
    without_keywords: str | None = Query(
        None, description="Exclude keyword IDs (comma-separated)"
    ),
    language: str | None = Query(
        None, description="Language code (e.g., 'en', 'hi', 'te')"
    ),
    region: str | None = Query(None, description="Region code (e.g., 'US', 'IN')"),
    release_year: int | None = Query(None, description="Release year"),
    release_date_gte: str | None = Query(
        None, description="Release date >= (YYYY-MM-DD)"
    ),
    release_date_lte: str | None = Query(
        None, description="Release date <= (YYYY-MM-DD)"
    ),
    vote_average_gte: float | None = Query(
        None, ge=0, le=10, description="Minimum vote average"
    ),
    vote_average_lte: float | None = Query(
        None, ge=0, le=10, description="Maximum vote average"
    ),
    vote_count_gte: int | None = Query(None, ge=0, description="Minimum vote count"),
    with_runtime_gte: int | None = Query(
        None, ge=0, description="Minimum runtime (minutes)"
    ),
    with_runtime_lte: int | None = Query(
        None, ge=0, description="Maximum runtime (minutes)"
    ),
    include_adult: bool | None = Query(False, description="Include adult content"),
    sort_by: str | None = Query("popularity.desc", description="Sort order"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Discover movies with extensive filtering capabilities.
    This is the power-user endpoint for complex movie queries.
    """
    try:
        per_page = max(1, min(per_page, 100))

        search_params = MovieSearchParams(
            with_genres=with_genres,
            without_genres=without_genres,
            with_keywords=with_keywords,
            without_keywords=without_keywords,
            with_original_language=language,
            region=region,
            year=release_year,
            primary_release_date_gte=release_date_gte,
            primary_release_date_lte=release_date_lte,
            vote_average_gte=vote_average_gte,
            vote_average_lte=vote_average_lte,
            vote_count_gte=vote_count_gte,
            with_runtime_gte=with_runtime_gte,
            with_runtime_lte=with_runtime_lte,
            include_adult=include_adult,
            sort_by=sort_by,
        )

        tmdb_client = await get_tmdb_client()

        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        tmdb_page_start = start_index // TMDB_PAGE_SIZE + 1
        tmdb_page_end = max(tmdb_page_start, (end_index - 1) // TMDB_PAGE_SIZE + 1)

        aggregated_movies = []
        total_results = 0
        tmdb_total_pages = None

        for tmdb_page in range(tmdb_page_start, tmdb_page_end + 1):
            params_with_page = search_params.model_copy(update={"page": tmdb_page})
            discover_response = await tmdb_client.discover_movies(params_with_page)

            if not discover_response:
                break

            if tmdb_total_pages is None:
                tmdb_total_pages = discover_response.pagination.total_pages
                total_results = discover_response.pagination.total_results
                if start_index >= total_results:
                    pagination = create_pagination_info(page, per_page, total_results)
                    return PaginatedResponse(data=[], pagination=pagination)

            aggregated_movies.extend(discover_response.movies or [])

            if tmdb_total_pages is not None and tmdb_page >= tmdb_total_pages:
                break

        if not aggregated_movies:
            pagination = create_pagination_info(page, per_page, total_results)
            return PaginatedResponse(data=[], pagination=pagination)

        relative_start = start_index - (tmdb_page_start - 1) * TMDB_PAGE_SIZE
        relative_start = max(relative_start, 0)
        relative_end = relative_start + per_page
        selected_movies = aggregated_movies[relative_start:relative_end]

        if not selected_movies:
            pagination = create_pagination_info(page, per_page, total_results)
            return PaginatedResponse(data=[], pagination=pagination)

        # Extract TMDB IDs from selected movies
        tmdb_ids = [movie.tmdb_id for movie in selected_movies]

        # Check which movies already exist in our database
        existing_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        existing_tmdb_ids_set = {movie.tmdb_id for movie in existing_movies}

        # Find missing movies (not in DB)
        missing_movies = [
            movie
            for movie in selected_movies
            if movie.tmdb_id not in existing_tmdb_ids_set
        ]

        # Use Processor 1: Insert lightweight + queue for background hydration
        if missing_movies:
            await insert_from_list_and_queue(
                db, missing_movies, queue_for_hydration=True
            )

            # Fetch all movies (including newly inserted)
            all_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        else:
            all_movies = existing_movies

        # Create ordered list matching original TMDB response order
        movie_by_tmdb_id = {movie.tmdb_id: movie for movie in all_movies}
        ordered_movies = [
            movie_by_tmdb_id[tmdb_id]
            for tmdb_id in tmdb_ids
            if tmdb_id in movie_by_tmdb_id
        ]

        movie_items = [
            MovieListItem(
                id=movie.id,
                tmdb_id=movie.tmdb_id,
                title=movie.title,
                overview=movie.overview,
                backdrop_path=movie.backdrop_path,
                poster_path=movie.poster_path,
                adult=movie.adult,
                popularity=movie.popularity,
                vote_average=movie.vote_average,
                release_date=movie.release_date.isoformat()
                if movie.release_date
                else None,
            )
            for movie in ordered_movies
        ]

        pagination = create_pagination_info(page, per_page, total_results)

        return PaginatedResponse(data=movie_items, pagination=pagination)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to discover movies: {e!s}",
        ) from e


@router.get("/search", response_model=PaginatedResponse[MovieListItem])
async def search_movies_db(
    query: str = Query(..., description="Search query (movie title or keywords)"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    include_adult: bool = Query(False, description="Include adult content"),
    year: int | None = Query(None, description="Filter by release year"),
    min_rating: float | None = Query(
        None, ge=0, le=10, description="Minimum vote average"
    ),
    with_genres: str | None = Query(None, description="Genre IDs (comma-separated)"),
    language: str | None = Query(
        None, description="Original language code (e.g., 'en', 'hi')"
    ),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Search movies in local database only (no TMDB API calls).

    Fast search across your database with multiple filters.
    """
    try:
        # Build query
        query_stmt = select(Movie).where(
            or_(
                Movie.title.ilike(f"%{query}%"),
                Movie.overview.ilike(f"%{query}%"),
                Movie.original_title.ilike(f"%{query}%"),
            )
        )

        # Apply filters
        if not include_adult:
            query_stmt = query_stmt.where(~Movie.adult)

        if year:
            query_stmt = query_stmt.where(
                func.extract("year", Movie.release_date) == year
            )

        if min_rating is not None:
            query_stmt = query_stmt.where(Movie.vote_average >= min_rating)

        if language:
            query_stmt = query_stmt.where(Movie.original_language == language)

        if with_genres:
            genre_ids = [int(gid.strip()) for gid in with_genres.split(",")]
            # Join with movie_genres to filter by genres
            query_stmt = (
                query_stmt.join(MovieGenre, Movie.id == MovieGenre.movie_id)
                .where(MovieGenre.genre_id.in_(genre_ids))
                .distinct()
            )

        # Count total results
        count_stmt = select(func.count()).select_from(query_stmt.subquery())
        total_result = await db.execute(count_stmt)
        total_results = total_result.scalar() or 0

        if total_results == 0:
            return PaginatedResponse(
                data=[],
                pagination=create_pagination_info(page, per_page, 0),
            )

        # Apply pagination and ordering
        offset = calculate_offset(page, per_page)
        query_stmt = (
            query_stmt.order_by(Movie.popularity.desc()).offset(offset).limit(per_page)
        )

        # Execute query
        result = await db.execute(query_stmt)
        movies = result.scalars().all()

        # Convert to response format
        movie_items = [
            MovieListItem(
                id=movie.id,
                tmdb_id=movie.tmdb_id,
                title=movie.title,
                overview=movie.overview,
                backdrop_path=movie.backdrop_path,
                poster_path=movie.poster_path,
                adult=movie.adult,
                popularity=movie.popularity,
                vote_average=movie.vote_average,
                release_date=movie.release_date.isoformat()
                if movie.release_date
                else None,
            )
            for movie in movies
        ]

        pagination = create_pagination_info(page, per_page, total_results)

        return PaginatedResponse(data=movie_items, pagination=pagination)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search local database: {e!s}",
        ) from e


@router.get("/search/tmdb", response_model=PaginatedResponse[MovieListItem])
async def search_movies_tmdb(
    query: str = Query(..., description="Search query (movie title or keywords)"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    include_adult: bool = Query(False, description="Include adult content"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Search movies from TMDB and insert into database (with background hydration)."""
    try:
        per_page = max(1, min(per_page, 100))

        tmdb_client = await get_tmdb_client()

        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        tmdb_page_start = start_index // TMDB_PAGE_SIZE + 1
        tmdb_page_end = max(tmdb_page_start, (end_index - 1) // TMDB_PAGE_SIZE + 1)

        aggregated_movies = []
        total_results = 0
        tmdb_total_pages = None

        for tmdb_page in range(tmdb_page_start, tmdb_page_end + 1):
            search_response = await tmdb_client.search_movies(
                query=query,
                page=tmdb_page,
            )

            if not search_response:
                break

            if tmdb_total_pages is None:
                tmdb_total_pages = search_response.pagination.total_pages
                total_results = search_response.pagination.total_results
                if start_index >= total_results:
                    pagination = create_pagination_info(page, per_page, total_results)
                    return PaginatedResponse(data=[], pagination=pagination)

            aggregated_movies.extend(search_response.movies or [])

            if tmdb_total_pages is not None and tmdb_page >= tmdb_total_pages:
                break

        if not aggregated_movies:
            pagination = create_pagination_info(page, per_page, total_results)
            return PaginatedResponse(data=[], pagination=pagination)

        relative_start = start_index - (tmdb_page_start - 1) * TMDB_PAGE_SIZE
        relative_start = max(relative_start, 0)
        relative_end = relative_start + per_page
        selected_movies = aggregated_movies[relative_start:relative_end]

        if not selected_movies:
            pagination = create_pagination_info(page, per_page, total_results)
            return PaginatedResponse(data=[], pagination=pagination)

        # Extract TMDB IDs from selected movies
        tmdb_ids = [movie.tmdb_id for movie in selected_movies]

        # Check which movies already exist in our database
        existing_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        existing_tmdb_ids_set = {movie.tmdb_id for movie in existing_movies}

        # Find missing movies (not in DB)
        missing_movies = [
            movie
            for movie in selected_movies
            if movie.tmdb_id not in existing_tmdb_ids_set
        ]

        # Use Processor 1: Insert lightweight + queue for background hydration
        if missing_movies:
            await insert_from_list_and_queue(
                db, missing_movies, queue_for_hydration=True
            )

            # Fetch all movies (including newly inserted)
            all_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        else:
            all_movies = existing_movies

        # Create ordered list matching original TMDB response order
        movie_by_tmdb_id = {movie.tmdb_id: movie for movie in all_movies}
        ordered_movies = [
            movie_by_tmdb_id[tmdb_id]
            for tmdb_id in tmdb_ids
            if tmdb_id in movie_by_tmdb_id
        ]

        movie_items = [
            MovieListItem(
                id=movie.id,
                tmdb_id=movie.tmdb_id,
                title=movie.title,
                overview=movie.overview,
                backdrop_path=movie.backdrop_path,
                poster_path=movie.poster_path,
                adult=movie.adult,
                popularity=movie.popularity,
                vote_average=movie.vote_average,
                release_date=movie.release_date.isoformat()
                if movie.release_date
                else None,
            )
            for movie in ordered_movies
        ]

        pagination = create_pagination_info(page, per_page, total_results)

        return PaginatedResponse(data=movie_items, pagination=pagination)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search movies: {e!s}",
        ) from e
