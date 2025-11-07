from datetime import UTC, datetime
from enum import Enum

from sqlmodel import Field, SQLModel


class JobType(str, Enum):
    MOVIE_DISCOVERY = "movie_discovery"
    CHANGE_TRACKING = "change_tracking"
    DATASET_EXPORT = "dataset_export"


class JobExecutionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobStatusBase(SQLModel):
    job_type: JobType = Field(description="Type of job being executed")
    status: JobExecutionStatus = Field(
        default=JobExecutionStatus.PENDING, description="Current execution status"
    )
    started_at: datetime | None = Field(default=None, description="Job start timestamp")
    completed_at: datetime | None = Field(
        default=None, description="Job completion timestamp"
    )
    total_items: int | None = Field(default=None, description="Total items to process")
    processed_items: int = Field(default=0, description="Items processed so far")
    failed_items: int = Field(default=0, description="Items that failed processing")


class JobStatus(JobStatusBase, table=True):
    __tablename__ = "job_status"

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Record creation timestamp",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Record update timestamp"
    )


class JobStatusCreate(JobStatusBase):
    pass


class JobStatusUpdate(SQLModel):
    status: JobExecutionStatus | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    total_items: int | None = None
    processed_items: int | None = None
    failed_items: int | None = None


class JobStatusRead(JobStatusBase):
    id: int
    created_at: datetime
    updated_at: datetime
