from typing import Optional
from datetime import datetime
from enum import Enum
from sqlmodel import SQLModel, Field


class JobType(str, Enum):
    MOVIE_DISCOVERY = "movie_discovery"
    CHANGE_TRACKING = "change_tracking"


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
    started_at: Optional[datetime] = Field(
        default=None, description="Job start timestamp"
    )
    completed_at: Optional[datetime] = Field(
        default=None, description="Job completion timestamp"
    )
    total_items: Optional[int] = Field(
        default=None, description="Total items to process"
    )
    processed_items: int = Field(default=0, description="Items processed so far")
    failed_items: int = Field(default=0, description="Items that failed processing")


class JobStatus(JobStatusBase, table=True):
    __tablename__ = "job_status"

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record update timestamp"
    )


class JobStatusCreate(JobStatusBase):
    pass


class JobStatusUpdate(SQLModel):
    status: Optional[JobExecutionStatus] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_items: Optional[int] = None
    processed_items: Optional[int] = None
    failed_items: Optional[int] = None


class JobStatusRead(JobStatusBase):
    id: int
    created_at: datetime
    updated_at: datetime
