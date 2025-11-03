from typing import List, Optional, Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.tmdb import get_tmdb_client
from app.api.deps import verify_token
from app.services.category_service import category_service, CATEGORY_CONFIGS
from app.services.tmdb_client.models import MovieSearchParams
from app.crud.movie import movie as movie_crud
from app.models.api_models import MovieListItem, GenreDict
from app.models.genre import Genre
from app.utils.pagination import (
    PaginatedResponse,
    create_pagination_info,
)
from sqlmodel import select

router = APIRouter()


@router.get("/genres", response_model=List[GenreDict])
async def get_genres(
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token)
):
    """Get all available movie genres."""
    result = await db.execute(select(Genre))
    genres = result.scalars().all()
    
    return [
        GenreDict(id=genre.id, name=genre.name) 
        for genre in genres
    ]


@router.get("/categories", response_model=List[Dict[str, str]])
async def get_available_categories(token: dict = Depends(verify_token)):
    """Get list of all available dynamic categories."""
    return await category_service.get_available_categories()


@router.get("/category/{category_name}", response_model=PaginatedResponse[MovieListItem])
async def get_category_movies(
    category_name: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token)
):
    """Get movies for a specific category (trending, popular, bollywood, etc.)."""
    try:
        # Get movie IDs from category service
        movie_ids, metadata = await category_service.get_category_movies(
            db=db,
            category=category_name,
            page=page
        )
        
        if not movie_ids:
            return PaginatedResponse(
                data=[],
                pagination=create_pagination_info(page, per_page, 0)
            )
        
        # Get movie details from our database
        movies = await movie_crud.get_multi_by_ids(db, movie_ids)
        
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
        
        # Use metadata from category service for pagination
        total_results = metadata.get('total_results', len(movie_items))
        pagination = create_pagination_info(page, per_page, total_results)
        
        return PaginatedResponse(
            data=movie_items,
            pagination=pagination
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch category movies: {str(e)}"
        )


@router.get("/discover", response_model=PaginatedResponse[MovieListItem])
async def discover_movies(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    # Filtering parameters
    with_genres: Optional[str] = Query(None, description="Genre IDs (comma-separated)"),
    without_genres: Optional[str] = Query(None, description="Exclude genre IDs (comma-separated)"),
    with_keywords: Optional[str] = Query(None, description="Keyword IDs (comma-separated)"),
    without_keywords: Optional[str] = Query(None, description="Exclude keyword IDs (comma-separated)"),
    language: Optional[str] = Query(None, description="Language code (e.g., 'en', 'hi', 'te')"),
    region: Optional[str] = Query(None, description="Region code (e.g., 'US', 'IN')"),
    release_year: Optional[int] = Query(None, description="Release year"),
    release_date_gte: Optional[str] = Query(None, description="Release date >= (YYYY-MM-DD)"),
    release_date_lte: Optional[str] = Query(None, description="Release date <= (YYYY-MM-DD)"),
    vote_average_gte: Optional[float] = Query(None, ge=0, le=10, description="Minimum vote average"),
    vote_average_lte: Optional[float] = Query(None, ge=0, le=10, description="Maximum vote average"),
    vote_count_gte: Optional[int] = Query(None, ge=0, description="Minimum vote count"),
    with_runtime_gte: Optional[int] = Query(None, ge=0, description="Minimum runtime (minutes)"),
    with_runtime_lte: Optional[int] = Query(None, ge=0, description="Maximum runtime (minutes)"),
    include_adult: Optional[bool] = Query(False, description="Include adult content"),
    sort_by: Optional[str] = Query("popularity.desc", description="Sort order"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token)
):
    """
    Discover movies with extensive filtering capabilities.
    This is the power-user endpoint for complex movie queries.
    """
    try:
        # Build search parameters
        search_params = MovieSearchParams(
            page=page,
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
            sort_by=sort_by
        )
        
        # Use TMDB client directly for discover
        tmdb_client = await get_tmdb_client()
        discover_response = await tmdb_client.discover_movies(search_params)
        
        if not discover_response or not discover_response.movies:
            return PaginatedResponse(
                data=[],
                pagination=create_pagination_info(page, per_page, 0)
            )
        
        # Extract TMDB IDs
        tmdb_ids = [movie.tmdb_id for movie in discover_response.movies]
        
        # Check which movies we have in our DB
        existing_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        existing_tmdb_ids = {movie.tmdb_id: movie for movie in existing_movies}
        
        # Fetch missing movies from TMDB
        missing_tmdb_ids = [
            tmdb_id for tmdb_id in tmdb_ids 
            if tmdb_id not in existing_tmdb_ids
        ]
        
        if missing_tmdb_ids:
            from app.utils.movie_processor import process_movie_batch
            await process_movie_batch(
                db=db,
                tmdb_client=tmdb_client,
                movie_ids=missing_tmdb_ids,
                job_id=None,
                use_locks=False,
                cancel_event=None
            )
            
            # Re-fetch to get newly created movies
            all_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        else:
            all_movies = existing_movies
        
        # Maintain original order from TMDB
        ordered_movies = []
        movie_by_tmdb_id = {movie.tmdb_id: movie for movie in all_movies}
        
        for tmdb_id in tmdb_ids:
            if tmdb_id in movie_by_tmdb_id:
                ordered_movies.append(movie_by_tmdb_id[tmdb_id])
        
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
            for movie in ordered_movies
        ]
        
        # Use TMDB pagination info
        total_results = discover_response.pagination.total_results
        pagination = create_pagination_info(page, per_page, total_results)
        
        return PaginatedResponse(
            data=movie_items,
            pagination=pagination
        )
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to discover movies: {str(e)}"
        )


@router.get("/search", response_model=PaginatedResponse[MovieListItem])
async def search_movies(
    query: str = Query(..., description="Search query (movie title or keywords)"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    include_adult: bool = Query(False, description="Include adult content"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token)
):
    """Search movies by title or overview text."""
    try:
        tmdb_client = await get_tmdb_client()
        search_response = await tmdb_client.search_movies(
            query=query,
            page=page
        )
        
        if not search_response or not search_response.movies:
            return PaginatedResponse(
                data=[],
                pagination=create_pagination_info(page, per_page, 0)
            )
        
        # Process similar to discover endpoint
        tmdb_ids = [movie.tmdb_id for movie in search_response.movies]
        
        existing_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        existing_tmdb_ids = {movie.tmdb_id: movie for movie in existing_movies}
        
        missing_tmdb_ids = [
            tmdb_id for tmdb_id in tmdb_ids 
            if tmdb_id not in existing_tmdb_ids
        ]
        
        if missing_tmdb_ids:
            from app.utils.movie_processor import process_movie_batch
            await process_movie_batch(
                db=db,
                tmdb_client=tmdb_client,
                movie_ids=missing_tmdb_ids,
                job_id=None,
                use_locks=False,
                cancel_event=None
            )
            
            all_movies = await movie_crud.get_by_tmdb_ids(db, tmdb_ids)
        else:
            all_movies = existing_movies
        
        # Maintain search result order
        ordered_movies = []
        movie_by_tmdb_id = {movie.tmdb_id: movie for movie in all_movies}
        
        for tmdb_id in tmdb_ids:
            if tmdb_id in movie_by_tmdb_id:
                ordered_movies.append(movie_by_tmdb_id[tmdb_id])
        
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
            for movie in ordered_movies
        ]
        
        total_results = search_response.pagination.total_results
        pagination = create_pagination_info(page, per_page, total_results)
        
        return PaginatedResponse(
            data=movie_items,
            pagination=pagination
        )
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search movies: {str(e)}"
        )


@router.post("/category/{category_name}/invalidate")
async def invalidate_category_cache(
    category_name: str,
    token: dict = Depends(verify_token)
):
    """Invalidate cache for a specific category (admin endpoint)."""
    if category_name not in CATEGORY_CONFIGS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Category '{category_name}' not found"
        )
    
    await category_service.invalidate_category_cache(category_name)
    
    return {"message": f"Cache invalidated for category '{category_name}'"}


@router.post("/cache/clear")
async def clear_all_category_cache(token: dict = Depends(verify_token)):
    """Clear all category caches (admin endpoint)."""
    for category_name in CATEGORY_CONFIGS.keys():
        await category_service.invalidate_category_cache(category_name)
    
    return {"message": "All category caches cleared"}