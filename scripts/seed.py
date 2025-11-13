import asyncio
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.redis import redis_client
from app.crud import job_log, job_status
from app.models.job_status import JobType
from app.services.tmdb_client.client import TMDBClient
from app.utils.movie_processor import BatchProcessResult, fetch_and_insert_full

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("database_seeder")

# Seeding configuration
PAGES_PER_CATEGORY = 50
API_DELAY = 0.25


CATEGORY_CATALOG: dict[str, dict[str, str]] = {
    "trending": {
        "name": "Trending",
        "description": "Movies that are currently trending",
        "method": "get_trending_movies",
    },
    "popular": {
        "name": "Popular",
        "description": "Most popular movies",
        "method": "get_popular_movies",
    },
    "top_rated": {
        "name": "Top Rated",
        "description": "Highest rated movies",
        "method": "get_top_rated_movies",
    },
    "upcoming": {
        "name": "Upcoming",
        "description": "Movies coming soon to theaters",
        "method": "get_upcoming_movies",
    },
    "now_playing": {
        "name": "Now Playing",
        "description": "Movies currently in theaters",
        "method": "get_now_playing_movies",
    },
    "bollywood": {
        "name": "Bollywood",
        "description": "Hindi movies from India",
        "method": "get_bollywood_movies",
    },
    "tollywood": {
        "name": "Tollywood",
        "description": "Telugu movies from India",
        "method": "get_tollywood_movies",
    },
    "kollywood": {
        "name": "Kollywood",
        "description": "Tamil movies from India",
        "method": "get_kollywood_movies",
    },
    "mollywood": {
        "name": "Mollywood",
        "description": "Malayalam movies from India",
        "method": "get_mollywood_movies",
    },
    "sandalwood": {
        "name": "Sandalwood",
        "description": "Kannada movies from India",
        "method": "get_sandalwood_movies",
    },
    "hollywood": {
        "name": "Hollywood",
        "description": "English movies from Hollywood",
        "method": "get_hollywood_movies",
    },
}


def _enumerate_categories(
    categories: dict[str, dict[str, str]],
) -> list[tuple[int, str, dict[str, str]]]:
    return [
        (index, key, meta) for index, (key, meta) in enumerate(categories.items(), 1)
    ]


def _parse_category_tokens(
    tokens: Sequence[str],
    categories: dict[str, dict[str, str]],
) -> tuple[list[str], list[str]]:
    ordered = list(categories.keys())
    seen = set()
    selected: list[str] = []
    invalid: list[str] = []

    def resolve_token(token: str) -> str | None:
        token = token.strip()
        if not token:
            return None
        if token.isdigit():
            idx = int(token)
            if 1 <= idx <= len(ordered):
                return ordered[idx - 1]
            return None
        normal = token.lower()
        for key in ordered:
            meta = categories[key]
            if normal in {key.lower(), meta["name"].lower()}:
                return key
        return None

    normalized: list[str] = []
    for token in tokens:
        normalized.extend(part for part in token.replace(",", " ").split() if part)

    for token in normalized:
        match = resolve_token(token)
        if match:
            if match not in seen:
                seen.add(match)
                selected.append(match)
        else:
            invalid.append(token)

    return selected, invalid


def _prompt_for_category_selection(categories: dict[str, dict[str, str]]) -> list[str]:
    menu = _enumerate_categories(categories)
    print("Available categories to seed:")
    for index, _, meta in menu:
        print(f"  {index:>2}. {meta['name']}: {meta['description']}")

    while True:
        raw = input(
            "Enter category numbers or names (e.g. '1 8 10') or press Enter for all: "
        ).strip()
        if not raw:
            return [key for _, key, _ in menu]
        selected, invalid = _parse_category_tokens([raw], categories)
        if invalid:
            print(f"Unrecognized selections: {', '.join(invalid)}")
            continue
        if not selected:
            print("No valid categories selected, try again.")
            continue
        return selected


class DatabaseSeeder:
    def __init__(self, selected_keys: Sequence[str] | None = None):
        self.tmdb_client = None
        self.categories = CATEGORY_CATALOG
        self.selected_keys = self._resolve_selected_keys(selected_keys)
        self.active_categories: dict[str, dict[str, str]] = {
            key: self.categories[key] for key in self.selected_keys
        }

    def _resolve_selected_keys(self, selected_keys: Sequence[str] | None) -> list[str]:
        if not selected_keys:
            return list(self.categories.keys())

        resolved: list[str] = []
        seen = set()
        for token in selected_keys:
            key = token.strip().lower()
            if not key:
                continue
            if key.isdigit():
                idx = int(key)
                ordered = list(self.categories.keys())
                if 1 <= idx <= len(ordered):
                    candidate = ordered[idx - 1]
                else:
                    continue
            else:
                matches = {
                    cat_key
                    for cat_key, meta in self.categories.items()
                    if key in {cat_key.lower(), meta["name"].lower()}
                }
                if not matches:
                    continue
                candidate = next(iter(matches))

            if candidate not in seen:
                seen.add(candidate)
                resolved.append(candidate)

        if not resolved:
            raise ValueError("No valid categories were selected.")
        return resolved

    async def run(self):
        logger.info("Starting database seeding process...")

        selected_names = ", ".join(
            self.categories[key]["name"] for key in self.selected_keys
        )
        logger.info("Selected categories: %s", selected_names)

        # Initialize services
        await redis_client.initialize()
        self.tmdb_client = TMDBClient()

        async for db_session in get_session():
            job_id = None
            overall_result = BatchProcessResult()
            try:
                # Create job status for tracking
                total_estimated_movies = (
                    len(self.active_categories) * PAGES_PER_CATEGORY * 20
                )
                job_status_record = await job_status.create_job(
                    db_session,
                    job_type=JobType.MOVIE_DISCOVERY,  # Use movie discovery type
                    total_items=total_estimated_movies,
                )
                job_id = job_status_record.id

                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Starting database seeding - "
                        f"{len(self.active_categories)} categories, "
                        f"{PAGES_PER_CATEGORY} pages each"
                    ),
                )
                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Selected categories: {selected_names}",
                )

                # Mark job as running
                await job_status.start_job(db_session, job_id)

                # Seed movies for each category
                overall_result = BatchProcessResult()
                for _category_key, category_info in self.active_categories.items():
                    category_result = await self._seed_category_movies(
                        db_session,
                        job_id,
                        category_info["name"],
                        category_info["method"],
                    )
                    overall_result.attempted += category_result.attempted
                    overall_result.succeeded += category_result.succeeded
                    overall_result.failed += category_result.failed
                    overall_result.skipped_locked += category_result.skipped_locked

                # Complete the job
                await job_status.complete_job(
                    db_session,
                    job_id,
                    items_processed=overall_result.succeeded,
                    failed_items=overall_result.failed,
                )
                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Seeding completed! "
                        f"{overall_result.succeeded} succeeded, "
                        f"{overall_result.failed} failed out of "
                        f"{overall_result.attempted} attempted"
                        + (
                            f" ({overall_result.skipped_locked} skipped due to locks)"
                            if overall_result.skipped_locked
                            else ""
                        )
                    ),
                )

                logger.info(
                    "Database seeding completed! %d movies processed "
                    "successfully, %d failed%s.",
                    overall_result.succeeded,
                    overall_result.failed,
                    (
                        f", {overall_result.skipped_locked} skipped due to locks"
                        if overall_result.skipped_locked
                        else ""
                    ),
                )

            except Exception as e:
                await db_session.rollback()
                if job_id:
                    await job_status.fail_job(
                        db_session,
                        job_id,
                        processed_items=overall_result.succeeded,
                        failed_items=overall_result.failed,
                    )
                    await job_log.log_error(
                        db_session, job_id, f"Seeding failed: {e!s}"
                    )
                logger.error(f"Database seeding failed: {e!s}")
                raise
            finally:
                await redis_client.close()
                if self.tmdb_client:
                    await self.tmdb_client.close()
                break  # noqa: B012

    async def _seed_category_movies(
        self, db: AsyncSession, job_id: int, category_name: str, method_name: str
    ) -> BatchProcessResult:
        logger.info(f"Seeding movies from {category_name} category...")

        await job_log.log_info(
            db,
            job_id,
            f"Starting to seed {PAGES_PER_CATEGORY} pages from {category_name}",
        )

        category_result = BatchProcessResult()

        # Get TMDB client method
        if not hasattr(self.tmdb_client, method_name):
            error_msg = (
                f"TMDB client method '{method_name}' not found for {category_name}"
            )
            logger.error(error_msg)
            await job_log.log_error(db, job_id, error_msg)
            return category_result

        method = getattr(self.tmdb_client, method_name)

        # Process each page
        for page in range(1, PAGES_PER_CATEGORY + 1):
            try:
                logger.info(
                    f"Processing page {page}/{PAGES_PER_CATEGORY} for {category_name}"
                )

                # Get movies from TMDB
                movie_results = await method(page=page)

                if not movie_results or not movie_results.movies:
                    await job_log.log_warning(
                        db, job_id, f"No movies found for {category_name} page {page}"
                    )
                    continue

                # Extract movie IDs
                movie_ids = [movie.tmdb_id for movie in movie_results.movies]

                # Process the batch using new processor (Processor 2: discovery mode)
                page_result = BatchProcessResult()
                for movie_id in movie_ids:
                    page_result.attempted += 1
                    try:
                        movie_obj = await fetch_and_insert_full(
                            db,
                            self.tmdb_client,
                            movie_id,
                            hydration_source="seed",
                            job_id=job_id,
                        )
                        if movie_obj:
                            page_result.succeeded += 1
                        else:
                            page_result.skipped_existing += 1
                    except Exception as e:
                        page_result.failed += 1
                        logger.error(f"Error processing movie {movie_id}: {e!s}")
                        await job_log.log_error(
                            db, job_id, f"Error processing movie {movie_id}: {e!s}"
                        )

                category_result.attempted += page_result.attempted
                category_result.succeeded += page_result.succeeded
                category_result.failed += page_result.failed
                category_result.skipped_locked += page_result.skipped_existing

                await job_log.log_info(
                    db,
                    job_id,
                    (
                        f"Completed page {page} for {category_name}: "
                        f"{page_result.succeeded} succeeded, "
                        f"{page_result.failed} failed out of {page_result.attempted}"
                        + (
                            f" ({page_result.skipped_locked} skipped due to locks)"
                            if page_result.skipped_locked
                            else ""
                        )
                    ),
                )

                # Add delay between pages
                await asyncio.sleep(API_DELAY)

            except Exception as e:
                error_msg = f"Failed to process page {page} for {category_name}: {e!s}"
                logger.error(error_msg)
                await job_log.log_error(db, job_id, error_msg)
                await db.rollback()
                continue

        logger.info(
            "Completed seeding for %s: %d succeeded, %d failed out of %d",
            category_name,
            category_result.succeeded,
            category_result.failed,
            category_result.attempted,
        )
        return category_result


async def main(selected_category_keys: Sequence[str] | None):
    try:
        seeder = DatabaseSeeder(selected_category_keys)
        await seeder.run()
    except KeyboardInterrupt:
        logger.info("Seeding interrupted by user")
    except Exception as e:
        logger.error(f"Seeding failed: {e!s}")
        sys.exit(1)


if __name__ == "__main__":
    selected_keys: Sequence[str] | None = None

    argv_tokens = sys.argv[1:]
    if argv_tokens:
        try:
            selection, invalid = _parse_category_tokens(argv_tokens, CATEGORY_CATALOG)
            if invalid:
                print(f"Ignoring unrecognized selections: {', '.join(invalid)}")
            if selection:
                selected_keys = selection
            else:
                raise ValueError
        except ValueError:
            print("No valid categories provided via arguments.")
            if not sys.stdin.isatty():
                sys.exit(1)

    if selected_keys is None:
        if sys.stdin.isatty():
            selected_keys = _prompt_for_category_selection(CATEGORY_CATALOG)
        else:
            selected_keys = list(CATEGORY_CATALOG.keys())

    asyncio.run(main(selected_keys))
