import asyncio
import logging
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.job_execution import job_execution_manager
from app.core.settings import settings
from app.crud import job_log, job_status
from app.models.job_status import JobType
from app.models.movie import Movie
from app.services.storage.dataset_builder import DatasetCSVBuilder
from app.services.storage.dataset_writer import S3DatasetWriter, UploadResult

logger = logging.getLogger(__name__)


class DatasetExportJob:
    def __init__(self) -> None:
        self.job_type = JobType.DATASET_EXPORT
        self.config = settings.DATASET_EXPORT
        self.dataset_builder = DatasetCSVBuilder()

    async def run(self) -> None:
        job_id: int | None = None
        cancel_event: asyncio.Event | None = None
        temp_file_path: str | None = None
        processed_rows = 0

        async for db_session in get_session():
            try:
                logger.info("Starting dataset export job run")

                job_status_record = await job_status.create_job(
                    db_session, job_type=self.job_type, total_items=0
                )
                job_id = job_status_record.id
                cancel_event = await job_execution_manager.register(
                    job_id, self.job_type
                )

                await job_log.log_info(
                    db_session,
                    job_id,
                    "Dataset export job started",
                )

                await job_status.start_job(db_session, job_id)

                self._validate_configuration()

                total_movies = await self._get_movie_count(db_session)
                await job_status.update_total_items(db_session, job_id, total_movies)

                if cancel_event and cancel_event.is_set():
                    raise asyncio.CancelledError()

                await job_log.log_info(
                    db_session,
                    job_id,
                    f"Exporting {total_movies} movies to CSV",
                )

                with NamedTemporaryFile(
                    "w", newline="", delete=False, encoding="utf-8"
                ) as tmp_file:
                    temp_file_path = tmp_file.name

                processed_rows = await self.dataset_builder.write_movie_items(
                    db_session, temp_file_path, cancel_event
                )

                if cancel_event and cancel_event.is_set():
                    raise asyncio.CancelledError()

                timestamp = datetime.now()
                object_key = self._build_object_key(timestamp)

                writer = S3DatasetWriter(
                    bucket=self.config.bucket,
                    prefix=self.config.prefix,
                    file_name=self.config.file_name,
                    endpoint_url=self.config.endpoint_url,
                    access_key=self.config.access_key,
                    secret_key=self.config.secret_key,
                    region_name=self.config.region_name,
                    use_ssl=self.config.use_ssl,
                )

                upload_result: UploadResult = await writer.upload_file(
                    temp_file_path, object_key
                )

                latest_result: UploadResult | None = None
                try:
                    latest_result = await writer.copy_to_latest(object_key)
                except Exception:
                    await job_log.log_warning(
                        db_session,
                        job_id,
                        (
                            "Failed to update latest object; "
                            "dated snapshot uploaded successfully"
                        ),
                    )

                await job_status.complete_job(
                    db_session,
                    job_id,
                    items_processed=processed_rows,
                    failed_items=0,
                )

                await job_log.log_success(
                    db_session,
                    job_id,
                    self._build_success_message(
                        processed_rows,
                        object_key,
                        upload_result.version_id if upload_result else None,
                        latest_result.key if latest_result else None,
                        latest_result.version_id if latest_result else None,
                    ),
                )

                logger.info(
                    "Dataset export complete: %s records -> %s/%s (version %s)",
                    processed_rows,
                    self.config.bucket,
                    object_key,
                    upload_result.version_id if upload_result else None,
                )
                if latest_result:
                    logger.info(
                        "Latest dataset object updated at %s/%s (version %s)",
                        self.config.bucket,
                        latest_result.key,
                        latest_result.version_id,
                    )
                break

            except asyncio.CancelledError:
                logger.warning("Dataset export job cancellation requested")
                if job_id is not None:
                    await job_log.log_warning(
                        db_session,
                        job_id,
                        "Dataset export job cancelled",
                    )
                    await job_status.cancel_job(db_session, job_id)
                break
            except Exception as exc:
                logger.error("Dataset export job failed: %s", exc, exc_info=True)
                if job_id is not None:
                    await job_log.log_error(
                        db_session,
                        job_id,
                        f"Dataset export failed: {exc}",
                    )
                    await job_status.fail_job(db_session, job_id)
                await db_session.rollback()
                raise
            finally:
                if job_id is not None:
                    await job_execution_manager.unregister(job_id)
                if temp_file_path:
                    temp_path = Path(temp_file_path)
                    if temp_path.exists():
                        try:
                            temp_path.unlink()
                        except OSError:
                            logger.warning(
                                "Unable to delete temporary export file %s",
                                temp_file_path,
                            )

    def _validate_configuration(self) -> None:
        if not self.config.enabled:
            raise RuntimeError("Dataset export job is disabled via configuration")
        required = {
            "bucket": self.config.bucket,
            "access_key": self.config.access_key,
            "secret_key": self.config.secret_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(
                "Dataset export configuration missing required values: "
                + ", ".join(missing)
            )

    async def _get_movie_count(self, db: AsyncSession) -> int:
        result = await db.execute(select(func.count()).select_from(Movie))
        count = result.scalar_one()
        return int(count or 0)

    def _build_object_key(self, timestamp: datetime) -> str:
        prefix = (self.config.prefix or "").strip("/")
        parts = []
        if prefix:
            parts.append(prefix)
        parts.append(timestamp.strftime("%Y-%m-%d"))
        file_name = self.config.file_name or "movie_items.csv"
        parts.append(file_name)
        return "/".join(parts)

    def _build_success_message(
        self,
        processed_rows: int,
        object_key: str,
        version_id: str | None,
        latest_key: str | None,
        latest_version_id: str | None,
    ) -> str:
        bucket_key = f"{self.config.bucket}/{object_key}"
        message = f"Exported {processed_rows} movie records to {bucket_key}"
        if version_id:
            message += f" (version {version_id})"
        if latest_key:
            message += f"; updated latest object at {self.config.bucket}/{latest_key}"
            if latest_version_id:
                message += f" (version {latest_version_id})"
        return message


# Job instance for scheduler
dataset_export_job = DatasetExportJob()
