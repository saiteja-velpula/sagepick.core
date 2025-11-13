import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlmodel import func, select

from app.api.deps import verify_token
from app.core.db import get_session
from app.crud.movie import movie as movie_crud
from app.models.api_models import (
    GenreDict,
    KeywordDict,
    MovieFullDetail,
    MovieListItem,
)
from app.models.genre import Genre
from app.models.keyword import Keyword
from app.models.movie import Movie
from app.models.movie_genre import MovieGenre
from app.utils.pagination import (
    PaginatedResponse,
    calculate_offset,
    create_pagination_info,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# Movie Endpoints
@router.get("/", response_model=PaginatedResponse[MovieListItem])
async def get_movies(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    search: str | None = Query(None, description="Search in title or overview"),
    genre: str | None = Query(
        None, description="Filter by genre name (comma-separated for multiple)"
    ),
    exclude_genre: str | None = Query(
        None, description="Exclude movies matching this genre name (comma-separated)"
    ),
    min_popularity: float | None = Query(
        None, ge=0, description="Minimum popularity score"
    ),
    adult: bool | None = Query(None, description="Filter by adult content"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get paginated list of movies with essential fields only."""
    offset = calculate_offset(page, per_page)

    # Build the query
    query = select(Movie)
    count_query = select(func.count(Movie.id))

    # Apply filters
    if search:
        search_filter = Movie.title.ilike(f"%{search}%") | Movie.overview.ilike(
            f"%{search}%"
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    if genre:
        include_terms = [value.strip() for value in genre.split(",") if value.strip()]
        if include_terms:
            include_conditions = [
                Genre.name.ilike(f"%{value}%") for value in include_terms
            ]
            include_filter = (
                or_(*include_conditions)
                if len(include_conditions) > 1
                else include_conditions[0]
            )
            include_subquery = (
                select(MovieGenre.movie_id).join(Genre).where(include_filter).distinct()
            )
            query = query.where(Movie.id.in_(include_subquery))
            count_query = count_query.where(Movie.id.in_(include_subquery))

    if exclude_genre:
        exclude_terms = [
            value.strip() for value in exclude_genre.split(",") if value.strip()
        ]
        if exclude_terms:
            exclude_conditions = [
                Genre.name.ilike(f"%{value}%") for value in exclude_terms
            ]
            exclude_filter = (
                or_(*exclude_conditions)
                if len(exclude_conditions) > 1
                else exclude_conditions[0]
            )
            exclude_subquery = (
                select(MovieGenre.movie_id).join(Genre).where(exclude_filter).distinct()
            )
            query = query.where(~Movie.id.in_(exclude_subquery))
            count_query = count_query.where(~Movie.id.in_(exclude_subquery))

    if min_popularity is not None:
        popularity_filter = Movie.popularity >= min_popularity
        query = query.where(popularity_filter)
        count_query = count_query.where(popularity_filter)

    if adult is not None:
        adult_filter = Movie.adult == adult
        query = query.where(adult_filter)
        count_query = count_query.where(adult_filter)

    # Get total count
    count_result = await db.execute(count_query)
    total_items = count_result.scalar() or 0

    # Apply pagination and ordering
    query = query.order_by(Movie.popularity.desc()).offset(offset).limit(per_page)

    # Execute query
    result = await db.execute(query)
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
            release_date=movie.release_date.isoformat() if movie.release_date else None,
        )
        for movie in movies
    ]

    pagination = create_pagination_info(page, per_page, total_items)

    return PaginatedResponse(data=movie_items, pagination=pagination)


@router.get("/{movie_id}", response_model=MovieFullDetail)
async def get_movie_by_id(
    movie_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get movie by ID with all details including genres and keywords.

    If the movie is not hydrated, it will be hydrated synchronously before returning.
    """
    # Use eager loading to fetch movie with relationships in a single query
    query = (
        select(Movie)
        .options(selectinload(Movie.genres), selectinload(Movie.keywords))
        .where(Movie.id == movie_id)
    )

    result = await db.execute(query)
    movie_obj = result.scalar_one_or_none()

    if not movie_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Movie not found"
        )

    # Check if movie needs hydration
    if not movie_obj.is_hydrated:
        from app.core.tmdb import get_tmdb_client
        from app.utils.movie_processor import fetch_and_insert_full

        logger.info(f"Movie {movie_obj.tmdb_id} not hydrated, hydrating now...")

        # Hydrate synchronously (user is waiting for this specific movie)
        tmdb_client = await get_tmdb_client()
        hydrated_movie = await fetch_and_insert_full(
            db=db,
            tmdb_client=tmdb_client,
            tmdb_id=movie_obj.tmdb_id,
            hydration_source="user_request",
            job_id=None,
        )

        if hydrated_movie:
            # Refresh to get updated data with relationships
            query = (
                select(Movie)
                .options(selectinload(Movie.genres), selectinload(Movie.keywords))
                .where(Movie.id == movie_id)
            )
            result = await db.execute(query)
            movie_obj = result.scalar_one_or_none()
            logger.info(f"Movie {movie_obj.tmdb_id} hydrated successfully")
        else:
            logger.warning(
                f"Failed to hydrate movie {movie_obj.tmdb_id}, "
                "returning partial data"
            )

    # Convert to response format using eager-loaded relationships
    genres_dict = [
        GenreDict(id=genre.id, name=genre.name) for genre in movie_obj.genres
    ]
    keywords_dict = [
        KeywordDict(id=keyword.id, name=keyword.name) for keyword in movie_obj.keywords
    ]

    return MovieFullDetail(
        id=movie_obj.id,
        tmdb_id=movie_obj.tmdb_id,
        title=movie_obj.title,
        original_title=movie_obj.original_title,
        overview=movie_obj.overview,
        poster_path=movie_obj.poster_path,
        backdrop_path=movie_obj.backdrop_path,
        original_language=movie_obj.original_language,
        release_date=movie_obj.release_date,
        vote_average=movie_obj.vote_average,
        vote_count=movie_obj.vote_count,
        popularity=movie_obj.popularity,
        runtime=movie_obj.runtime,
        budget=movie_obj.budget,
        revenue=movie_obj.revenue,
        status=movie_obj.status,
        adult=movie_obj.adult,
        genres=genres_dict,
        keywords=keywords_dict,
    )


@router.get("/tmdb/{tmdb_id}", response_model=MovieFullDetail)
async def get_movie_by_tmdb_id(
    tmdb_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get movie by TMDB ID with all details."""
    movie_obj = await movie_crud.get_by_tmdb_id(db, tmdb_id)
    if not movie_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Movie not found"
        )

    # Redirect to get_movie_by_id for consistency
    return await get_movie_by_id(movie_obj.id, db)


# Statistics Endpoints
@router.get("/stats")
async def get_movie_statistics(
    db: AsyncSession = Depends(get_session), token: dict = Depends(verify_token)
):
    """Get movie database statistics."""
    # Total movies
    total_movies_query = select(func.count(Movie.id))
    total_movies_result = await db.execute(total_movies_query)
    total_movies = total_movies_result.scalar()

    # Total genres
    total_genres_query = select(func.count(Genre.id))
    total_genres_result = await db.execute(total_genres_query)
    total_genres = total_genres_result.scalar()

    # Total keywords
    total_keywords_query = select(func.count(Keyword.id))
    total_keywords_result = await db.execute(total_keywords_query)
    total_keywords = total_keywords_result.scalar()

    # Movies by adult content
    adult_movies_query = select(func.count(Movie.id)).where(Movie.adult)
    adult_movies_result = await db.execute(adult_movies_query)
    adult_movies = adult_movies_result.scalar()

    return {
        "total_movies": total_movies,
        "total_genres": total_genres,
        "total_keywords": total_keywords,
        "adult_movies": adult_movies,
        "non_adult_movies": total_movies - adult_movies,
    }
