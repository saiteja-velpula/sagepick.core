import asyncio
import logging
import sys
from pathlib import Path

from sqlalchemy import delete

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.db import async_session, close_db
from app.models.job_log import JobLog
from app.models.job_status import JobStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("remove_job")


async def remove_job(job_id: int) -> None:
    async with async_session() as session:
        status = await session.get(JobStatus, job_id)
        if not status:
            logger.warning("Job status %s not found.", job_id)
            return

        logger.info("Removing logs for job %s", job_id)
        await session.execute(delete(JobLog).where(JobLog.job_status_id == job_id))

        logger.info("Deleting job status %s", job_id)
        await session.delete(status)
        await session.commit()
        logger.info("Job %s removed.", job_id)


async def main(job_id: int) -> None:
    try:
        await remove_job(job_id)
    finally:
        await close_db()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/remove_job.py <job_id>")
        sys.exit(1)

    try:
        job_identifier = int(sys.argv[1])
    except ValueError:
        print("Job id must be an integer.")
        sys.exit(1)

    asyncio.run(main(job_identifier))
