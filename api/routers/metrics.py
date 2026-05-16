from __future__ import annotations

from fastapi import APIRouter, Depends

from api.dependencies import QueueDepend, RepoDepend
from api.middleware.auth import require_api_key
from core.models import JobStatus, Priority

router = APIRouter(prefix="/metrics", tags=["metrics"], dependencies=[Depends(require_api_key)])


@router.get("")
def get_metrics(repo: RepoDepend, queue: QueueDepend) -> dict:
    depths = queue.queue_depths()
    return {
        "queue_depths": {
            "high": depths[Priority.HIGH],
            "normal": depths[Priority.NORMAL],
            "low": depths[Priority.LOW],
        },
        "delayed_count": queue.delayed_count(),
        "jobs_by_status": {
            s: len(repo.list(status=s, limit=500))
            for s in (
                JobStatus.PENDING,
                JobStatus.RUNNING,
                JobStatus.DONE,
                JobStatus.FAILED,
                JobStatus.DEAD,
            )
        },
    }
