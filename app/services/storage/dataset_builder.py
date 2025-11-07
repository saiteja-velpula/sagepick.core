import asyncio
import csv
import logging
from collections.abc import Mapping
from datetime import date
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


MOVIE_EXPORT_QUERY = text(
    """
    SELECT
        m.id AS movie_id,
        m.tmdb_id,
        m.title,
        m.original_title,
        m.overview,
        m.release_date,
        m.original_language,
        m.runtime AS runtime_minutes,
        m.status,
        m.adult,
        m.vote_average,
        m.vote_count,
        m.popularity,
        m.budget AS budget_usd,
        m.revenue AS revenue_usd,
        COALESCE(g_data.genre_names, '') AS genres,
        COALESCE(g_data.genre_ids, '') AS genre_ids,
        COALESCE(g_data.genre_count, 0) AS genre_count,
        COALESCE(k_data.keyword_names, '') AS keywords,
        COALESCE(k_data.keyword_ids, '') AS keyword_ids,
        COALESCE(k_data.keyword_count, 0) AS keyword_count
    FROM movies m
    LEFT JOIN LATERAL (
        SELECT
            string_agg(g.name, '|' ORDER BY g.name) AS genre_names,
            string_agg(g.tmdb_id::text, '|' ORDER BY g.tmdb_id::text) AS genre_ids,
            COUNT(*) AS genre_count
        FROM movie_genres mg
        JOIN genres g ON g.id = mg.genre_id
        WHERE mg.movie_id = m.id
    ) g_data ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            string_agg(k.name, '|' ORDER BY k.name) AS keyword_names,
            string_agg(k.tmdb_id::text, '|' ORDER BY k.tmdb_id::text) AS keyword_ids,
            COUNT(*) AS keyword_count
        FROM movie_keywords mk
        JOIN keywords k ON k.id = mk.keyword_id
        WHERE mk.movie_id = m.id
    ) k_data ON TRUE
    ORDER BY m.id
    """
)


CSV_FIELDNAMES = [
    "movie_id",
    "tmdb_id",
    "title",
    "original_title",
    "overview",
    "release_date",
    "original_language",
    "runtime_minutes",
    "status",
    "adult",
    "vote_average",
    "vote_count",
    "popularity",
    "budget_usd",
    "revenue_usd",
    "genres",
    "genre_ids",
    "genre_count",
    "keywords",
    "keyword_ids",
    "keyword_count",
]


class DatasetCSVBuilder:
    """Builds the movie_items CSV by streaming from the database."""

    def __init__(self) -> None:
        self.fieldnames = CSV_FIELDNAMES

    async def write_movie_items(
        self,
        db: AsyncSession,
        output_path: str,
        cancel_event: asyncio.Event | None = None,
    ) -> int:
        processed = 0

        def write_header():
            with Path(output_path).open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(
                    csv_file, fieldnames=self.fieldnames, extrasaction="ignore"
                )
                writer.writeheader()

        def write_row(row_data: dict):
            with Path(output_path).open("a", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(
                    csv_file, fieldnames=self.fieldnames, extrasaction="ignore"
                )
                writer.writerow(row_data)

        await asyncio.to_thread(write_header)

        result = await db.stream(MOVIE_EXPORT_QUERY)
        async for row in result.mappings():
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError()
            formatted_row = self._format_row(row)
            await asyncio.to_thread(write_row, formatted_row)
            processed += 1

        logger.debug("CSV build complete: wrote %s rows to %s", processed, output_path)
        return processed

    def _format_row(self, row: Mapping[str, object]) -> dict:
        release_date = row.get("release_date")
        if isinstance(release_date, date):
            release_value = release_date.isoformat()
        elif release_date is None:
            release_value = ""
        else:
            release_value = str(release_date)

        return {
            "movie_id": row.get("movie_id"),
            "tmdb_id": row.get("tmdb_id"),
            "title": row.get("title", ""),
            "original_title": row.get("original_title", ""),
            "overview": row.get("overview", ""),
            "release_date": release_value,
            "original_language": row.get("original_language", ""),
            "runtime_minutes": row.get("runtime_minutes")
            if row.get("runtime_minutes") is not None
            else "",
            "status": row.get("status", ""),
            "adult": bool(row.get("adult", False)),
            "vote_average": row.get("vote_average")
            if row.get("vote_average") is not None
            else "",
            "vote_count": row.get("vote_count")
            if row.get("vote_count") is not None
            else "",
            "popularity": row.get("popularity")
            if row.get("popularity") is not None
            else "",
            "budget_usd": row.get("budget_usd")
            if row.get("budget_usd") is not None
            else "",
            "revenue_usd": row.get("revenue_usd")
            if row.get("revenue_usd") is not None
            else "",
            "genres": row.get("genres", ""),
            "genre_ids": row.get("genre_ids", ""),
            "genre_count": row.get("genre_count") or 0,
            "keywords": row.get("keywords", ""),
            "keyword_ids": row.get("keyword_ids", ""),
            "keyword_count": row.get("keyword_count") or 0,
        }
