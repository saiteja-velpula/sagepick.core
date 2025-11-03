from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, delete

from app.core.db import get_session
from app.core.scheduler import job_scheduler
from app.core.redis import redis_client
from app.core.job_execution import job_execution_manager
from app.crud.job_status import job_status
from app.crud.job_log import job_log
from app.models.job_status import JobType, JobExecutionStatus, JobStatusRead
from app.models.job_log import JobLogRead, LogLevel
from app.api.deps import verify_token

router = APIRouter()


# Scheduler Status Endpoints
@router.get("/scheduler/status")
async def get_scheduler_status(token: dict = Depends(verify_token)):
    """Get the current status of the job scheduler."""
    return {
        "is_running": job_scheduler.is_running,
        "jobs": job_scheduler.get_all_jobs_status(),
    }


@router.post("/scheduler/{job_id}/trigger")
async def trigger_job_manually(job_id: str, token: dict = Depends(verify_token)):
    """Manually trigger a job to run immediately."""
    valid_job_ids = [
        "movie_discovery_job",
        "change_tracking_job",
    ]

    if job_id not in valid_job_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid job_id. Must be one of: {valid_job_ids}",
        )

    success = await job_scheduler.trigger_job_manually(job_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger job: {job_id}",
        )

    return {"message": f"Job {job_id} triggered successfully"}


@router.post("/scheduler/{job_id}/pause")
async def pause_job(job_id: str, token: dict = Depends(verify_token)):
    """Pause a scheduled job."""
    success = job_scheduler.pause_job(job_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to pause job: {job_id}",
        )

    return {"message": f"Job {job_id} paused successfully"}


@router.post("/scheduler/{job_id}/resume")
async def resume_job(job_id: str, token: dict = Depends(verify_token)):
    """Resume a paused job."""
    success = job_scheduler.resume_job(job_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume job: {job_id}",
        )

    return {"message": f"Job {job_id} resumed successfully"}


# Job Status Endpoints
@router.get("/status", response_model=List[JobStatusRead])
async def get_recent_job_statuses(
    limit: int = Query(50, ge=1, le=200),
    job_type: Optional[JobType] = Query(None, description="Filter by job type"),
    status_filter: Optional[JobExecutionStatus] = Query(
        None, alias="status", description="Filter by execution status"
    ),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get recent job execution statuses with optional filtering."""
    query = select(job_status.model)

    if job_type:
        query = query.where(job_status.model.job_type == job_type)

    if status_filter:
        query = query.where(job_status.model.status == status_filter)

    query = query.order_by(job_status.model.created_at.desc()).limit(limit)

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/status/running", response_model=List[JobStatusRead])
async def get_running_jobs(
    db: AsyncSession = Depends(get_session), token: dict = Depends(verify_token)
):
    """Get all currently running jobs."""
    return await job_status.get_running_jobs(db)


@router.get("/status/failed", response_model=List[JobStatusRead])
async def get_failed_jobs(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get recently failed jobs."""
    statement = (
        select(job_status.model)
        .where(job_status.model.status == JobExecutionStatus.FAILED)
        .order_by(job_status.model.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(statement)
    return result.scalars().all()


@router.get("/status/completed", response_model=List[JobStatusRead])
async def get_completed_jobs(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get recently completed jobs."""
    statement = (
        select(job_status.model)
        .where(job_status.model.status == JobExecutionStatus.COMPLETED)
        .order_by(job_status.model.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(statement)
    return result.scalars().all()


@router.get("/status/type/{job_type}", response_model=List[JobStatusRead])
async def get_job_statuses_by_type(
    job_type: JobType,
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get job statuses by job type."""
    return await job_status.get_jobs_by_type(db, job_type, limit=limit)


@router.get("/status/{job_id}", response_model=JobStatusRead)
async def get_job_status(
    job_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get status of a specific job execution."""
    job_status_obj = await job_status.get(db, job_id)

    if not job_status_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job status not found"
        )

    return job_status_obj


@router.delete("/status/{job_id}")
async def delete_job_status(
    job_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Delete a job status record and its associated logs."""
    job_status_obj = await job_status.get(db, job_id)

    if not job_status_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job status not found"
        )

    # Check if job is currently running
    if job_status_obj.status == JobExecutionStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete a running job",
        )

    # Delete associated logs first
    logs_statement = delete(job_log.model).where(job_log.model.job_status_id == job_id)
    await db.execute(logs_statement)

    # Delete the job status
    await job_status.remove(db, id=job_id)

    return {"message": f"Job status {job_id} and its logs have been deleted"}


@router.post("/status/{job_id}/cancel")
async def cancel_running_job(
    job_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Cancel an actively running job execution."""
    job_status_obj = await job_status.get(db, job_id)
    if not job_status_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job status not found"
        )

    if job_status_obj.status != JobExecutionStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job is not currently running",
        )

    cancellation_initiated = await job_execution_manager.cancel(job_id)
    if not cancellation_initiated:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Job is finishing up and can no longer be cancelled",
        )

    await job_log.log_warning(db, job_id, "Cancellation requested for running job")

    return {"message": f"Cancellation requested for job {job_id}"}


# Job Logs Endpoints
@router.get("/logs", response_model=List[JobLogRead])
async def get_recent_job_logs(
    limit: int = Query(100, ge=1, le=1000),
    level: Optional[LogLevel] = Query(None, description="Filter by log level"),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get recent job logs across all jobs with optional filtering."""
    if level:
        # Add method to get logs by level - for now use existing error method for ERROR level
        if level == LogLevel.ERROR:
            return await job_log.get_error_logs(db, limit=limit)
        else:
            # Fallback to recent logs for other levels
            return await job_log.get_recent_logs(db, limit=limit)
    return await job_log.get_recent_logs(db, limit=limit)


@router.get("/logs/errors", response_model=List[JobLogRead])
async def get_error_logs(
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get recent error logs."""
    return await job_log.get_error_logs(db, limit=limit)


@router.get("/logs/job/{job_status_id}", response_model=List[JobLogRead])
async def get_job_logs(
    job_status_id: int,
    db: AsyncSession = Depends(get_session),
    token: dict = Depends(verify_token),
):
    """Get all logs for a specific job execution."""
    logs = await job_log.get_logs_by_job_id(db, job_status_id)
    if not logs:
        # Check if job status exists
        job_status_obj = await job_status.get(db, job_status_id)
        if not job_status_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Job status not found"
            )
    return logs


# Job Management Endpoints
@router.post("/scheduler/start")
async def start_scheduler(token: dict = Depends(verify_token)):
    """Start the job scheduler if it's not running."""
    if job_scheduler.is_running:
        return {"message": "Scheduler is already running"}

    try:
        await job_scheduler.start()
        return {"message": "Scheduler started successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start scheduler: {str(e)}",
        )


@router.post("/scheduler/stop")
async def stop_scheduler(token: dict = Depends(verify_token)):
    """Stop the job scheduler."""
    if not job_scheduler.is_running:
        return {"message": "Scheduler is not running"}

    try:
        await job_scheduler.stop()
        return {"message": "Scheduler stopped successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop scheduler: {str(e)}",
        )


@router.get("/types")
async def get_job_types(token: dict = Depends(verify_token)):
    """Get all available job types."""
    return {
        "job_types": [
            {"name": job_type.name, "value": job_type.value} for job_type in JobType
        ],
        "execution_statuses": [
            {"name": status.name, "value": status.value}
            for status in JobExecutionStatus
        ],
    }


# Redis/System Health Endpoints
@router.get("/health")
async def get_system_health(token: dict = Depends(verify_token)):
    """Get overall system health status."""
    try:
        # Initialize Redis if needed
        if not redis_client._initialized:
            await redis_client.initialize()

        redis_healthy = await redis_client.health_check()

        return {
            "scheduler": {
                "running": job_scheduler.is_running,
                "jobs_count": len(job_scheduler.get_all_jobs_status()),
            },
            "redis": {
                "healthy": redis_healthy,
                "initialized": redis_client._initialized,
            },
            "overall_status": "healthy"
            if (job_scheduler.is_running and redis_healthy)
            else "degraded",
        }

    except Exception as e:
        return {
            "scheduler": {
                "running": job_scheduler.is_running,
                "jobs_count": len(job_scheduler.get_all_jobs_status())
                if job_scheduler.is_running
                else 0,
            },
            "redis": {"healthy": False, "error": str(e)},
            "overall_status": "unhealthy",
        }


@router.get("/stats")
async def get_job_statistics(
    db: AsyncSession = Depends(get_session), token: dict = Depends(verify_token)
):
    """Get job execution statistics."""
    # Get recent jobs (last 100)
    recent_jobs = await job_status.get_recent_jobs(db, limit=100)

    # Calculate statistics
    total_jobs = len(recent_jobs)
    completed_jobs = len(
        [j for j in recent_jobs if j.status == JobExecutionStatus.COMPLETED]
    )
    failed_jobs = len([j for j in recent_jobs if j.status == JobExecutionStatus.FAILED])
    running_jobs = len(
        [j for j in recent_jobs if j.status == JobExecutionStatus.RUNNING]
    )

    # Job type breakdown
    job_type_stats = {}
    for job_type in JobType:
        type_jobs = [j for j in recent_jobs if j.job_type == job_type]
        job_type_stats[job_type.value] = {
            "total": len(type_jobs),
            "completed": len(
                [j for j in type_jobs if j.status == JobExecutionStatus.COMPLETED]
            ),
            "failed": len(
                [j for j in type_jobs if j.status == JobExecutionStatus.FAILED]
            ),
            "running": len(
                [j for j in type_jobs if j.status == JobExecutionStatus.RUNNING]
            ),
        }

    return {
        "summary": {
            "total_jobs": total_jobs,
            "completed": completed_jobs,
            "failed": failed_jobs,
            "running": running_jobs,
            "success_rate": round(
                (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0, 2
            ),
        },
        "by_job_type": job_type_stats,
        "scheduler_info": {
            "is_running": job_scheduler.is_running,
            "scheduled_jobs": job_scheduler.get_all_jobs_status(),
        },
    }
