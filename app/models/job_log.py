from datetime import datetime
from enum import Enum

from sqlmodel import Field, SQLModel


class LogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


class JobLogBase(SQLModel):
    job_status_id: int = Field(
        foreign_key="job_status.id", description="Reference to job status"
    )
    level: LogLevel = Field(default=LogLevel.INFO, description="Log level")
    message: str = Field(description="Log message")


class JobLog(JobLogBase, table=True):
    __tablename__ = "job_logs"

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(), description="Log entry timestamp"
    )


class JobLogCreate(JobLogBase):
    pass


class JobLogRead(JobLogBase):
    id: int
    created_at: datetime
