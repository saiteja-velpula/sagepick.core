import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from app.models.job_status import JobType

logger = logging.getLogger(__name__)


@dataclass
class RunningJob:
    job_type: JobType
    task: asyncio.Task
    cancel_event: asyncio.Event


class JobExecutionManager:
    """Tracks live job executions and supports cooperative cancellation."""

    def __init__(self) -> None:
        self._jobs: Dict[int, RunningJob] = {}
        self._lock = asyncio.Lock()

    async def register(self, job_status_id: int, job_type: JobType) -> asyncio.Event:
        """Register the currently running task for the given job status id."""
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("register() must be called from within an asyncio task")

        cancel_event = asyncio.Event()
        async with self._lock:
            self._jobs[job_status_id] = RunningJob(
                job_type=job_type, task=task, cancel_event=cancel_event
            )
        logger.debug("Registered running job %s (%s)", job_status_id, job_type.value)
        return cancel_event

    async def unregister(self, job_status_id: int) -> None:
        """Remove a job execution from tracking."""
        async with self._lock:
            self._jobs.pop(job_status_id, None)
        logger.debug("Unregistered job %s", job_status_id)

    async def cancel(self, job_status_id: int) -> bool:
        """Request cancellation of a running job.

        Returns True if the job was found and cancellation was initiated, False otherwise.
        """
        async with self._lock:
            record = self._jobs.get(job_status_id)
            if not record:
                return False
            if record.task.done():
                self._jobs.pop(job_status_id, None)
                return False
            record.cancel_event.set()
            record.task.cancel()
            logger.info(
                "Cancellation requested for job %s (%s)",
                job_status_id,
                record.job_type.value,
            )
            return True

    async def get_running_job(self, job_status_id: int) -> Optional[RunningJob]:
        async with self._lock:
            return self._jobs.get(job_status_id)


job_execution_manager = JobExecutionManager()
