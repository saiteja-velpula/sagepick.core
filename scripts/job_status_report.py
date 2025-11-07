import asyncio
import logging
import sys
from collections import Counter
from datetime import UTC
from pathlib import Path

from sqlmodel import select

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.db import async_session, close_db
from app.models.job_status import JobExecutionStatus, JobStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("job_status_report")


async def fetch_recent_jobs(limit: int) -> list[JobStatus]:
    async with async_session() as session:
        result = await session.execute(
            select(JobStatus).order_by(JobStatus.created_at.desc()).limit(limit)
        )
        return list(result.scalars().all())


def _format_timestamp(value) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%SZ")


async def main(limit: int) -> None:
    try:
        jobs = await fetch_recent_jobs(limit)
        if not jobs:
            print("No job statuses found.")
            return

        status_counts = Counter(job.status for job in jobs)
        print("Summary by status:")
        for status in JobExecutionStatus:
            count = status_counts.get(status, 0)
            print(f"  {status.value.title():<10}: {count}")
        print()

        header = (
            f"{'ID':>6}  {'Type':<18}  {'Status':<10}  "
            f"{'Processed/Total':<18}  {'Failed':>6}  "
            f"{'Created (UTC)':<20}  {'Updated (UTC)':<20}"
        )
        print(header)
        print("-" * len(header))
        for job in jobs:
            processed = job.processed_items or 0
            total = job.total_items if job.total_items is not None else "-"
            print(
                f"{job.id:>6}  {job.job_type.value:<18}  "
                f"{job.status.value:<10}  "
                f"{processed}/{total:<14}  {job.failed_items or 0:>6}  "
                f"{_format_timestamp(job.created_at):<20}  "
                f"{_format_timestamp(job.updated_at):<20}"
            )
    finally:
        await close_db()


if __name__ == "__main__":
    limit = 20
    if len(sys.argv) > 1:
        try:
            limit = max(1, int(sys.argv[1]))
        except ValueError:
            print("Usage: python scripts/job_status_report.py [limit]")
            sys.exit(1)

    asyncio.run(main(limit))
