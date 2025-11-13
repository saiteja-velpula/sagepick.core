import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.job_execution import job_execution_manager
from app.core.redis import redis_client
from app.core.settings import settings
from app.core.tmdb import get_tmdb_client
from app.crud import job_log, job_status
from app.models.job_status import JobType
from app.utils.movie_processor import BatchProcessResult, fetch_and_upsert_full

logger = logging.getLogger(__name__)


class ChangeTrackingJob:
    def __init__(self):
        self.job_type = JobType.CHANGE_TRACKING
        self.config = settings.JOBS

    async def run(self):
        """Main job execution method."""
        job_id = None
        cancel_event = None

        async for db_session in get_session():
            try:
                # Create job status record (we don't know total items yet)
                job_status_record = await job_status.create_job(
                    db_session, job_type=self.job_type, total_items=0
                )
                job_id = job_status_record.id
                cancel_event = await job_execution_manager.register(
                    job_id, self.job_type
                )

                # Log job start
                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Starting Change Tracking Job - "
                        "fetching all changed movies from last 24 hours"
                    ),
                )

                # Mark job as running
                await job_status.start_job(db_session, job_id)

                # Initialize Redis client
                await redis_client.initialize()

                # Get shared TMDB client
                tmdb_client = await get_tmdb_client()

                # Track changes and process movies
                batch_result = await self._track_changes(
                    db_session, job_id, tmdb_client, cancel_event
                )

                await job_log.log_info(
                    db_session,
                    job_id,
                    (
                        "Change tracking summary: "
                        f"{batch_result.succeeded} succeeded, "
                        f"{batch_result.failed} failed "
                        f"out of {batch_result.attempted} attempts"
                        + (
                            f" ({batch_result.skipped_locked} skipped due to locks)"
                            if batch_result.skipped_locked
                            else ""
                        )
                    ),
                )

                failure_rate = (
                    batch_result.failed / batch_result.attempted
                    if batch_result.attempted > 0
                    else 0
                )

                if (
                    batch_result.attempted > 0
                    and failure_rate >= self.config.error_rate_threshold
                ):
                    await job_log.log_error(
                        db_session,
                        job_id,
                        (
                            "Change tracking encountered a high failure rate "
                            f"({failure_rate:.0%}); marking job as failed"
                        ),
                    )
                    await job_status.fail_job(
                        db_session,
                        job_id,
                        processed_items=batch_result.succeeded,
                        failed_items=batch_result.failed,
                    )
                    logger.error(
                        "Change Tracking Job failed due to error rate %.0f%%",
                        failure_rate * 100,
                    )
                else:
                    await job_status.complete_job(
                        db_session,
                        job_id,
                        items_processed=batch_result.succeeded,
                        failed_items=batch_result.failed,
                    )

                    logger.info(
                        (
                            "Change Tracking Job completed successfully. "
                            "Processed %d changed movies."
                        ),
                        batch_result.succeeded,
                    )

                break
            except asyncio.CancelledError:
                logger.warning("Change Tracking Job cancellation requested")
                if job_id:
                    await job_log.log_warning(
                        db_session,
                        job_id,
                        "Cancellation requested; aborting change tracking run",
                    )
                    await job_status.cancel_job(db_session, job_id)
                return
            except Exception as e:
                await db_session.rollback()
                logger.error(f"Change Tracking Job failed: {e!s}", exc_info=True)

                if job_id:
                    await job_log.log_error(
                        db_session, job_id, f"Job failed with error: {e!s}"
                    )
                    await job_status.fail_job(db_session, job_id)

                raise
            finally:
                if job_id is not None:
                    await job_execution_manager.unregister(job_id)

    async def _track_changes(
        self,
        db: AsyncSession,
        job_id: int,
        tmdb_client,
        cancel_event: asyncio.Event | None,
    ) -> BatchProcessResult:
        """Track changes from TMDB changes endpoint."""
        try:
            total_attempted = 0
            total_succeeded = 0
            total_failed = 0
            total_skipped_locked = 0
            current_page = 1
            total_pages = 1

            while current_page <= total_pages:
                if cancel_event and cancel_event.is_set():
                    await job_log.log_warning(
                        db,
                        job_id,
                        (
                            "Cancellation requested; "
                            "stopping remaining change tracking pages"
                        ),
                    )
                    break

                await job_log.log_info(
                    db, job_id, f"Fetching changes page {current_page}/{total_pages}"
                )

                # Fetch changes from TMDB
                changes_response = await tmdb_client.get_movie_changes(
                    page=current_page
                )

                if not changes_response or not changes_response.results:
                    await job_log.log_warning(
                        db, job_id, f"No changes found on page {current_page}"
                    )
                    break

                # Update total pages info
                total_pages = (
                    changes_response.total_pages if changes_response.total_pages else 1
                )
                changed_movies = changes_response.results
                await job_log.log_info(
                    db,
                    job_id,
                    (
                        f"Found {len(changed_movies)} changed movies "
                        f"on page {current_page}/{total_pages}"
                    ),
                )

                # Update total items estimate if this is the first page
                if current_page == 1:
                    estimated_total = total_pages * self.config.tracking_items_per_page
                    await job_status.update_total_items(db, job_id, estimated_total)

                # Process each changed movie
                movie_ids = [
                    movie_data.id for movie_data in changed_movies if movie_data.id
                ]

                # Process movies using Processor 3 (always update)
                if movie_ids:
                    for movie_id in movie_ids:
                        if cancel_event and cancel_event.is_set():
                            await job_log.log_warning(
                                db,
                                job_id,
                                "Cancellation requested; stopping movie processing",
                            )
                            break

                        # Acquire lock
                        from app.core.redis import redis_client

                        lock_acquired = await redis_client.acquire_movie_lock(movie_id)
                        if not lock_acquired:
                            total_skipped_locked += 1
                            continue

                        try:
                            total_attempted += 1
                            processed_movie = await fetch_and_upsert_full(
                                db, tmdb_client, movie_id, job_id=job_id
                            )

                            if processed_movie:
                                total_succeeded += 1
                            else:
                                total_failed += 1

                        except Exception as e:
                            total_failed += 1
                            await job_log.log_error(
                                db, job_id, f"Error processing movie {movie_id}: {e!s}"
                            )
                        finally:
                            await redis_client.release_movie_lock(movie_id)

                    # Update job status
                    if total_succeeded or total_failed:
                        await job_status.increment_counts(
                            db,
                            job_id,
                            processed_delta=total_succeeded,
                            failed_delta=total_failed,
                        )

                    if total_skipped_locked:
                        await job_log.log_info(
                            db,
                            job_id,
                            (
                                f"Skipped {total_skipped_locked} "
                                f"changed movies on page {current_page} "
                                "due to existing locks"
                            ),
                        )

                current_page += 1

                # Log progress every 10 pages
                if current_page % 10 == 0:
                    await job_log.log_info(
                        db,
                        job_id,
                        (
                            f"Progress: Processed {total_succeeded} movies, "
                            f"on page {current_page}/{total_pages}"
                        ),
                    )

            return BatchProcessResult(
                attempted=total_attempted,
                succeeded=total_succeeded,
                failed=total_failed,
                skipped_locked=total_skipped_locked,
            )

        except Exception as e:
            await job_log.log_error(db, job_id, f"Error in _track_changes: {e!s}")
            await db.rollback()
            raise


# Job instance for scheduler
change_tracking_job = ChangeTrackingJob()
