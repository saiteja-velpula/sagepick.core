from typing import List
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.job_log import JobLog, JobLogCreate, LogLevel


class CRUDJobLog(CRUDBase[JobLog, JobLogCreate, JobLogCreate]):
    async def log_info(
        self, db: AsyncSession, job_status_id: int, message: str
    ) -> JobLog:
        log_create = JobLogCreate(
            job_status_id=job_status_id, level=LogLevel.INFO, message=message
        )
        return await self.create(db, obj_in=log_create)

    async def log_warning(
        self, db: AsyncSession, job_status_id: int, message: str
    ) -> JobLog:
        log_create = JobLogCreate(
            job_status_id=job_status_id, level=LogLevel.WARNING, message=message
        )
        return await self.create(db, obj_in=log_create)

    async def log_error(
        self, db: AsyncSession, job_status_id: int, message: str
    ) -> JobLog:
        log_create = JobLogCreate(
            job_status_id=job_status_id, level=LogLevel.ERROR, message=message
        )
        return await self.create(db, obj_in=log_create)

    async def get_logs_by_job_id(
        self, db: AsyncSession, job_status_id: int
    ) -> List[JobLog]:
        statement = (
            select(JobLog)
            .where(JobLog.job_status_id == job_status_id)
            .order_by(JobLog.created_at.asc())
        )
        result = await db.execute(statement)
        return result.scalars().all()

    async def get_recent_logs(self, db: AsyncSession, limit: int = 100) -> List[JobLog]:
        statement = select(JobLog).order_by(JobLog.created_at.desc()).limit(limit)
        result = await db.execute(statement)
        return result.scalars().all()

    async def get_error_logs(self, db: AsyncSession, limit: int = 50) -> List[JobLog]:
        statement = (
            select(JobLog)
            .where(JobLog.level == LogLevel.ERROR)
            .order_by(JobLog.created_at.desc())
            .limit(limit)
        )
        result = await db.execute(statement)
        return result.scalars().all()


# Singleton instance
job_log = CRUDJobLog(JobLog)
