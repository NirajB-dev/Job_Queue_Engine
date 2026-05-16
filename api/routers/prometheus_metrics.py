from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api.dependencies import QueueDepend, RepoDepend
from core.metrics import DELAYED_JOBS, JOBS_BY_STATUS, QUEUE_DEPTH
from core.models import JobStatus, Priority

router = APIRouter()

_ALL_STATUSES = [
    JobStatus.PENDING,
    JobStatus.RUNNING,
    JobStatus.DONE,
    JobStatus.FAILED,
    JobStatus.DEAD,
]


@router.get("/prometheus", include_in_schema=False)
def prometheus_scrape(repo: RepoDepend, queue: QueueDepend) -> Response:
    """Prometheus scrape endpoint — updates gauges then emits the full registry."""
    depths = queue.queue_depths()
    QUEUE_DEPTH.labels("high").set(depths[Priority.HIGH])
    QUEUE_DEPTH.labels("normal").set(depths[Priority.NORMAL])
    QUEUE_DEPTH.labels("low").set(depths[Priority.LOW])
    DELAYED_JOBS.set(queue.delayed_count())

    counts = repo.count_by_status()
    for status in _ALL_STATUSES:
        JOBS_BY_STATUS.labels(status).set(counts.get(status, 0))

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
