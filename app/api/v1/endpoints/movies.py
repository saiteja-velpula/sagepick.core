"""
Movies API endpoints for SAGEPICK movie data management.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, func
from sqlalchemy import or_
from sqlalchemy.orm import selectinload

from app.core.db import get_session
from app.crud.movie import movie as movie_crud
from app.crud.media_category import media_category as media_category_crud
from app.models.movie import Movie
from app.models.media_category import MediaCategory, MediaCategoryRead
from app.models.media_category_movie import MediaCategoryMovie
from app.models.genre import Genre
from app.models.keyword import Keyword
from app.models.movie_genre import MovieGenre
from app.api.deps import verify_token
from app.models.api_models import (
    MovieListItem,
    MovieFullDetail,
    GenreDict,
    KeywordDict,
)
from app.utils.pagination import (
    PaginatedResponse,
    create_pagination_info,
    calculate_offset,
)

router = APIRouter()


# Movie Endpoints
@router.get("/movies", response_model=PaginatedResponse[MovieListItem])
async def get_movies(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search in title or overview"),
    genre: Optional[str] = Query(
        None, description="Filter by genre name (comma-separated for multiple)"
    ),
    exclude_genre: Optional[str] = Query(
        None, description="Exclude movies matching this genre name (comma-separated)"
    ),
    min_popularity: Optional[float] = Query(
        None, ge=0, description="Minimum popularity score"
    ),
    adult: Optional[bool] = Query(None, description="Filter by adult content"),
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


@router.get("/movies/{movie_id}", response_model=MovieFullDetail)
async def get_movie_by_id(
    movie_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get movie by ID with all details including genres and keywords."""
    # Use eager loading to fetch movie with relationships in a single query
    query = (
        select(Movie)
        .options(
            selectinload(Movie.genres),
            selectinload(Movie.keywords)
        )
        .where(Movie.id == movie_id)
    )
    
    result = await db.execute(query)
    movie_obj = result.scalar_one_or_none()
    
    if not movie_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Movie not found"
        )

    # Convert to response format using eager-loaded relationships
    genres_dict = [GenreDict(id=genre.id, name=genre.name) for genre in movie_obj.genres]
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


@router.get("/movies/tmdb/{tmdb_id}", response_model=MovieFullDetail)
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


# Movie Categories Endpoints
@router.get("/categories", response_model=List[MediaCategoryRead])
async def get_movie_categories(
    db: AsyncSession = Depends(get_session), token: dict = Depends(verify_token)
):
    """Get all movie categories."""
    return await media_category_crud.get_all_categories(db)


@router.get("/categories/{category_id}", response_model=MediaCategoryRead)
async def get_movie_category(
    category_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get a specific movie category."""
    category = await media_category_crud.get(db, category_id)
    if not category:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Category not found"
        )
    return category


@router.get(
    "/categories/{category_id}/movies", response_model=PaginatedResponse[MovieListItem]
)
async def get_movies_by_category(
    category_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get movies in a specific category with pagination."""
    # Check if category exists
    category = await media_category_crud.get(db, category_id)
    if not category:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Category not found"
        )

    offset = calculate_offset(page, per_page)

    # Query movies in category
    movies_query = (
        select(Movie)
        .join(MediaCategoryMovie)
        .where(MediaCategoryMovie.media_category_id == category_id)
        .order_by(Movie.popularity.desc())
        .offset(offset)
        .limit(per_page)
    )

    # Count total movies in category
    count_query = (
        select(func.count(Movie.id))
        .join(MediaCategoryMovie)
        .where(MediaCategoryMovie.media_category_id == category_id)
    )

    # Execute queries
    movies_result = await db.execute(movies_query)
    movies = movies_result.scalars().all()

    count_result = await db.execute(count_query)
    total_items = count_result.scalar()

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


@router.get(
    "/categories/name/{category_name}/movies",
    response_model=PaginatedResponse[MovieListItem],
)
async def get_movies_by_category_name(
    category_name: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get movies in a category by category name with pagination."""
    # Get category by name
    category = await media_category_crud.get_by_name(db, category_name)
    if not category:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Category '{category_name}' not found",
        )

    # Redirect to get_movies_by_category
    return await get_movies_by_category(category.id, page, per_page, db)


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

    # Total categories
    total_categories_query = select(func.count(MediaCategory.id))
    total_categories_result = await db.execute(total_categories_query)
    total_categories = total_categories_result.scalar()

    # Movies by adult content
    adult_movies_query = select(func.count(Movie.id)).where(Movie.adult)
    adult_movies_result = await db.execute(adult_movies_query)
    adult_movies = adult_movies_result.scalar()

    return {
        "total_movies": total_movies,
        "total_genres": total_genres,
        "total_keywords": total_keywords,
        "total_categories": total_categories,
        "adult_movies": adult_movies,
        "non_adult_movies": total_movies - adult_movies,
    }
