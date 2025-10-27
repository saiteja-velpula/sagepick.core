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
logger = logging.getLogger("purge_job_data")


async def purge_job_tables() -> None:
    async with async_session() as session:
        logger.info("Deleting all job logs and job statuses…")
        await session.execute(delete(JobLog))
        await session.execute(delete(JobStatus))
        await session.commit()
        logger.info("Job logs and statuses removed.")


async def main() -> None:
    try:
        await purge_job_tables()
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
