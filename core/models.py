from __future__ import annotations

import uuid
from datetime import datetime
from enum import IntEnum
from typing import Any

from pydantic import BaseModel, Field


class Priority(IntEnum):
    HIGH = 0
    NORMAL = 1
    LOW = 2


class JobStatus(str):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"


class Job(BaseModel):
    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: Priority = Priority.NORMAL
    status: str = JobStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    idempotency_key: str | None = None
    scheduled_at: datetime
    run_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    worker_id: str | None = None

    model_config = {"from_attributes": True}


class JobAttempt(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    attempt: int
    started_at: datetime
    failed_at: datetime
    error: str
    worker_id: str

    model_config = {"from_attributes": True}


class CreateJobRequest(BaseModel):
    type: str
    payload: dict[str, Any]
    priority: Priority = Priority.NORMAL
    max_attempts: int = Field(default=3, ge=1, le=10)
    idempotency_key: str | None = None
    run_at: datetime | None = None
