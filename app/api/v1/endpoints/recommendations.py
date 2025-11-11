from datetime import date

from fastapi import APIRouter, Body, Depends, Query
from sqlalchemy import and_, case, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func, select

from app.api.deps import verify_token
from app.core.db import get_session
from app.core.logging import get_structured_logger
from app.models.api_models import (
    ColdStartPreferences,
    RankedMovieItem,
    ReleaseYearRange,
)
from app.models.keyword import Keyword
from app.models.movie import Movie
from app.models.movie_genre import MovieGenre
from app.models.movie_keyword import MovieKeyword
from app.utils.pagination import (
    PaginatedResponse,
    calculate_offset,
    create_pagination_info,
)

logger = get_structured_logger(__name__)

router = APIRouter()

# Ranking weights
GENRE_MATCH_WEIGHT = 10.0
LANGUAGE_MATCH_WEIGHT = 5.0
YEAR_RANGE_MATCH_WEIGHT = 3.0
KEYWORD_MATCH_WEIGHT = 2.0

# Minimum vote count threshold for quality filtering
MIN_VOTE_COUNT = 50
MIN_VOTE_AVERAGE = 5.0


def get_year_range_filter(
    year_range: ReleaseYearRange,
) -> tuple[date | None, date | None]:
    """Convert year range enum to date filters."""
    if year_range == ReleaseYearRange.MODERN:
        # 2020-present
        return date(2020, 1, 1), None
    elif year_range == ReleaseYearRange.RECENT:
        # 2010-2019
        return date(2010, 1, 1), date(2019, 12, 31)
    elif year_range == ReleaseYearRange.CLASSIC:
        # 1990-2009
        return date(1990, 1, 1), date(2009, 12, 31)
    elif year_range == ReleaseYearRange.RETRO:
        # Before 1990
        return None, date(1989, 12, 31)
    else:  # ALL
        return None, None


@router.post("/cold-start", response_model=PaginatedResponse[RankedMovieItem])
async def get_cold_start_recommendations(
    preferences: ColdStartPreferences = Body(...),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    include_adult: bool = Query(False, description="Include adult content"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """
    Get personalized movie recommendations for new users based on their preferences.

    This endpoint implements a rule-based ranking algorithm that scores movies based on:
    - Genre matching (highest weight)
    - Language matching
    - Release year range matching
    - Keyword matching (optional, lower weight)
    - Popularity as a tie-breaker

    Ideal for cold-start problem (users with 0-14 interactions).
    """
    try:
        offset = calculate_offset(page, per_page)

        # Build year range filters
        year_conditions = []
        for year_range in preferences.release_year_ranges:
            start_date, end_date = get_year_range_filter(year_range)

            if start_date and end_date:
                year_conditions.append(
                    and_(
                        Movie.release_date >= start_date, Movie.release_date <= end_date
                    )
                )
            elif start_date:
                year_conditions.append(Movie.release_date >= start_date)
            elif end_date:
                year_conditions.append(Movie.release_date <= end_date)
            else:  # ALL range - no filter
                year_conditions.append(True)

        year_filter = or_(*year_conditions) if year_conditions else True

        # Calculate ranking score using SQL expressions
        # Genre matching score
        genre_match_subquery = (
            select(func.count(MovieGenre.genre_id))
            .where(
                and_(
                    MovieGenre.movie_id == Movie.id,
                    MovieGenre.genre_id.in_(preferences.genre_ids),
                )
            )
            .scalar_subquery()
        )

        # Language matching score
        language_match_case = case(
            (Movie.original_language.in_(preferences.languages), LANGUAGE_MATCH_WEIGHT),
            else_=0.0,
        )

        # Year range matching score (already in filter, so all matches get the weight)
        year_match_score = YEAR_RANGE_MATCH_WEIGHT

        # Keyword matching score (if provided)
        if preferences.keywords:
            # Get keyword IDs from names
            keyword_query = select(Keyword.id).where(
                or_(*[Keyword.name.ilike(f"%{kw}%") for kw in preferences.keywords])
            )
            keyword_result = await db.execute(keyword_query)
            keyword_ids = [row[0] for row in keyword_result.all()]

            if keyword_ids:
                keyword_match_subquery = (
                    select(func.count(MovieKeyword.keyword_id))
                    .where(
                        and_(
                            MovieKeyword.movie_id == Movie.id,
                            MovieKeyword.keyword_id.in_(keyword_ids),
                        )
                    )
                    .scalar_subquery()
                )
                keyword_score = keyword_match_subquery * KEYWORD_MATCH_WEIGHT
            else:
                keyword_score = 0.0
        else:
            keyword_score = 0.0

        # Calculate total rank score
        rank_score = (
            (genre_match_subquery * GENRE_MATCH_WEIGHT)
            + language_match_case
            + year_match_score
            + keyword_score
        )

        # Build the main query with all filters
        query = select(Movie, rank_score.label("rank_score")).where(
            and_(
                # Must match at least one genre
                Movie.id.in_(
                    select(MovieGenre.movie_id).where(
                        MovieGenre.genre_id.in_(preferences.genre_ids)
                    )
                ),
                # Year range filter
                year_filter,
                # Adult content filter
                Movie.adult == include_adult if not include_adult else True,
                # Quality filters
                Movie.vote_count >= MIN_VOTE_COUNT,
                Movie.vote_average >= MIN_VOTE_AVERAGE,
            )
        )

        # Count query for pagination
        count_query = select(func.count(Movie.id)).where(
            and_(
                Movie.id.in_(
                    select(MovieGenre.movie_id).where(
                        MovieGenre.genre_id.in_(preferences.genre_ids)
                    )
                ),
                year_filter,
                Movie.adult == include_adult if not include_adult else True,
                Movie.vote_count >= MIN_VOTE_COUNT,
                Movie.vote_average >= MIN_VOTE_AVERAGE,
            )
        )

        # Get total count
        count_result = await db.execute(count_query)
        total_items = count_result.scalar() or 0

        if total_items == 0:
            logger.info(
                "No movies found matching cold-start preferences",
                extra={
                    "genre_ids": preferences.genre_ids,
                    "languages": preferences.languages,
                    "year_ranges": preferences.release_year_ranges,
                },
            )
            return PaginatedResponse(
                data=[],
                pagination=create_pagination_info(page, per_page, total_items),
            )

        # Order by rank score DESC, then popularity DESC
        query = (
            query.order_by(rank_score.desc(), Movie.popularity.desc())
            .offset(offset)
            .limit(per_page)
        )

        # Execute query
        result = await db.execute(query)
        rows = result.all()

        # Convert to response format
        movie_items = [
            RankedMovieItem(
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
                rank_score=float(rank_score_value),
            )
            for movie, rank_score_value in rows
        ]

        pagination = create_pagination_info(page, per_page, total_items)

        logger.info(
            "Cold-start recommendations generated",
            extra={
                "total_results": total_items,
                "page": page,
                "returned_items": len(movie_items),
                "preferences": preferences.model_dump(),
            },
        )

        return PaginatedResponse(data=movie_items, pagination=pagination)

    except Exception as e:
        logger.error(
            f"Failed to generate cold-start recommendations: {e!s}",
            exc_info=True,
            extra={"preferences": preferences.model_dump() if preferences else None},
        )
        raise
