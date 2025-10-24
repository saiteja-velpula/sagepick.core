from typing import List, Optional
from datetime import datetime
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models.job_status import (
    JobStatus, 
    JobStatusCreate, 
    JobStatusUpdate,
    JobType,
    JobExecutionStatus
)


class CRUDJobStatus(CRUDBase[JobStatus, JobStatusCreate, JobStatusUpdate]):
    
    async def create_job(
        self, 
        db: AsyncSession, 
        *, 
        job_type: JobType, 
        total_items: Optional[int] = None
    ) -> JobStatus:
        job_create = JobStatusCreate(
            job_type=job_type,
            total_items=total_items,
            status=JobExecutionStatus.PENDING
        )
        return await self.create(db, obj_in=job_create)
    
    async def start_job(self, db: AsyncSession, job_id: int) -> Optional[JobStatus]:
        job_status = await self.get(db, job_id)
        if job_status:
            job_status.status = JobExecutionStatus.RUNNING
            job_status.started_at = datetime.utcnow()
            job_status.updated_at = datetime.utcnow()
            db.add(job_status)
            await db.commit()
            await db.refresh(job_status)
        return job_status
    
    async def complete_job(
        self, 
        db: AsyncSession, 
        job_id: int, 
        items_processed: Optional[int] = None,
        failed_items: Optional[int] = None
    ) -> Optional[JobStatus]:
        job_status = await self.get(db, job_id)
        if job_status:
            job_status.status = JobExecutionStatus.COMPLETED
            job_status.completed_at = datetime.utcnow()
            if items_processed is not None:
                job_status.processed_items = items_processed
            if failed_items is not None:
                job_status.failed_items = failed_items
            job_status.updated_at = datetime.utcnow()
            db.add(job_status)
            await db.commit()
            await db.refresh(job_status)
        return job_status
    
    async def fail_job(
        self,
        db: AsyncSession,
        job_id: int,
        processed_items: Optional[int] = None,
        failed_items: Optional[int] = None
    ) -> Optional[JobStatus]:
        job_status = await self.get(db, job_id)
        if job_status:
            job_status.status = JobExecutionStatus.FAILED
            job_status.completed_at = datetime.utcnow()
            if processed_items is not None:
                job_status.processed_items = processed_items
            if failed_items is not None:
                job_status.failed_items = failed_items
            job_status.updated_at = datetime.utcnow()
            db.add(job_status)
            await db.commit()
            await db.refresh(job_status)
        return job_status

    async def cancel_job(
        self,
        db: AsyncSession,
        job_id: int,
        processed_items: Optional[int] = None,
        failed_items: Optional[int] = None
    ) -> Optional[JobStatus]:
        job_status = await self.get(db, job_id)
        if job_status:
            job_status.status = JobExecutionStatus.CANCELLED
            job_status.completed_at = datetime.utcnow()
            if processed_items is not None:
                job_status.processed_items = processed_items
            if failed_items is not None:
                job_status.failed_items = failed_items
            job_status.updated_at = datetime.utcnow()
            db.add(job_status)
            await db.commit()
            await db.refresh(job_status)
        return job_status

    async def increment_counts(
        self,
        db: AsyncSession,
        job_id: int,
        processed_delta: int = 0,
        failed_delta: int = 0,
        *,
        commit: bool = False
    ) -> Optional[JobStatus]:
        """Increment processed/failed counters for a job and persist immediately."""
        if not processed_delta and not failed_delta:
            return await self.get(db, job_id)

        job_status = await self.get(db, job_id)
        if job_status:
            if processed_delta:
                job_status.processed_items += processed_delta
            if failed_delta:
                job_status.failed_items += failed_delta
            job_status.updated_at = datetime.utcnow()
            db.add(job_status)
            if commit:
                await db.commit()
                await db.refresh(job_status)
            else:
                await db.flush()
        return job_status
    
    async def update_total_items(
        self, 
        db: AsyncSession, 
        job_id: int, 
        total_items: int
    ) -> Optional[JobStatus]:
        job_status = await self.get(db, job_id)
        if job_status:
            job_status.total_items = total_items
            db.add(job_status)
            await db.commit()
            await db.refresh(job_status)
        return job_status
    
    async def get_running_jobs(self, db: AsyncSession) -> List[JobStatus]:
        statement = select(JobStatus).where(JobStatus.status == JobExecutionStatus.RUNNING)
        result = await db.execute(statement)
        return result.scalars().all()
    
    async def get_recent_jobs(self, db: AsyncSession, limit: int = 50) -> List[JobStatus]:
        statement = select(JobStatus).order_by(JobStatus.created_at.desc()).limit(limit)
        result = await db.execute(statement)
        return result.scalars().all()
    
    async def get_jobs_by_type(
        self, 
        db: AsyncSession, 
        job_type: JobType, 
        limit: int = 20
    ) -> List[JobStatus]:
        statement = select(JobStatus).where(JobStatus.job_type == job_type).order_by(JobStatus.created_at.desc()).limit(limit)
        result = await db.execute(statement)
        return result.scalars().all()


# Singleton instance
job_status = CRUDJobStatus(JobStatus)