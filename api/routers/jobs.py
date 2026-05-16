from __future__ import annotations

import time
import uuid

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.dependencies import QueueDepend, RepoDepend
from api.middleware.auth import require_api_key
from core.models import CreateJobRequest, Job, JobAttempt, JobStatus, Priority

router = APIRouter(prefix="/jobs", tags=["jobs"], dependencies=[Depends(require_api_key)])


@router.post("", response_model=Job, status_code=status.HTTP_201_CREATED)
def create_job(req: CreateJobRequest, repo: RepoDepend, queue: QueueDepend) -> Job:
    try:
        job = repo.create(req)
    except psycopg2.errors.UniqueViolation:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Duplicate idempotency_key")

    run_at_ts = job.run_at.timestamp()
    queue.enqueue(job.id, job.priority, run_at=run_at_ts)
    return job


@router.get("", response_model=list[Job])
def list_jobs(
    repo: RepoDepend,
    job_status: str | None = Query(None, alias="status"),
    job_type: str | None = Query(None, alias="type"),
    priority: int | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[Job]:
    parsed_priority = Priority(priority) if priority is not None else None
    return repo.list(
        status=job_status,
        job_type=job_type,
        priority=parsed_priority,
        limit=limit,
        offset=offset,
    )


@router.get("/{job_id}", response_model=Job)
def get_job(job_id: uuid.UUID, repo: RepoDepend) -> Job:
    job = repo.get_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


@router.get("/{job_id}/attempts", response_model=list[JobAttempt])
def get_attempts(job_id: uuid.UUID, repo: RepoDepend) -> list[JobAttempt]:
    job = repo.get_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return repo.get_attempts(job_id)


@router.post("/{job_id}/cancel", response_model=Job)
def cancel_job(job_id: uuid.UUID, repo: RepoDepend) -> Job:
    job = repo.get_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    if job.status not in (JobStatus.PENDING,):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot cancel a job with status '{job.status}'",
        )
    return repo.update_status(job_id, JobStatus.DEAD, error="Cancelled by API request")


@router.post("/{job_id}/requeue", response_model=Job)
def requeue_job(job_id: uuid.UUID, repo: RepoDepend, queue: QueueDepend) -> Job:
    try:
        job = repo.requeue(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    queue.enqueue(job.id, job.priority, run_at=time.time())
    return job
