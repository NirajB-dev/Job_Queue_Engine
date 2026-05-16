from __future__ import annotations

import logging
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import psycopg2
import redis as redis_lib

from core.metrics import JOB_DURATION, JOBS_FAILED, JOBS_PROCESSED
from core.models import JobStatus
from core.queue import RedisQueue
from core.registry import HandlerRegistry
from core.repository import JobRepository
from worker.retry import backoff_seconds

if TYPE_CHECKING:
    from core.models import Job

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 15  # seconds


class Worker:
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        redis_client: redis_lib.Redis,
        handler_registry: HandlerRegistry,
    ) -> None:
        self.worker_id = f"worker-{uuid.uuid4()}"
        self._running = True
        self._conn = conn
        self._queue = RedisQueue(redis_client)
        self._repo = JobRepository(conn)
        self._registry = handler_registry
        self._last_heartbeat: float = 0.0

    # ── main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        logger.info("Worker %s started", self.worker_id)
        try:
            while self._running:
                self._maybe_heartbeat()
                dequeued = self._queue.dequeue_blocking(timeout=5)
                if dequeued is None:
                    continue
                self._process(dequeued.job_id)
        finally:
            self._queue.deregister_worker(self.worker_id)
            self._conn.close()
            logger.info("Worker %s stopped", self.worker_id)

    def _handle_sigterm(self, sig, frame) -> None:
        logger.info("SIGTERM received — draining worker %s", self.worker_id)
        self._running = False

    def _maybe_heartbeat(self) -> None:
        now = time.time()
        if now - self._last_heartbeat >= HEARTBEAT_INTERVAL:
            self._queue.register_heartbeat(self.worker_id)
            self._last_heartbeat = now

    # ── job processing ────────────────────────────────────────────────────────

    def _process(self, job_id: uuid.UUID) -> None:
        job = self._repo.get_by_id(job_id)
        if job is None:
            logger.warning("Job %s not found in DB, skipping", job_id)
            return

        job = self._repo.update_status(
            job_id,
            JobStatus.RUNNING,
            worker_id=self.worker_id,
            attempts_increment=1,
        )
        started_at = datetime.now(timezone.utc)
        handler = self._registry.get(job.type)

        if handler is None:
            error = f"No handler registered for job type '{job.type}'"
            logger.error("Job %s: %s", job_id, error)
            self._repo.move_to_dlq(
                job_id,
                attempt=job.attempts,
                started_at=started_at,
                error=error,
                worker_id=self.worker_id,
            )
            JOBS_FAILED.labels(job_type=job.type, reason="no_handler").inc()
            return

        try:
            result = handler(job)
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()
            self._repo.update_status(
                job_id,
                JobStatus.DONE,
                result=result if result is not None else {},
            )
            JOB_DURATION.labels(job_type=job.type).observe(duration)
            JOBS_PROCESSED.labels(job_type=job.type).inc()
            logger.info("Job %s (%s) done in %.3fs", job_id, job.type, duration)
        except Exception as exc:
            error = str(exc)
            logger.warning(
                "Job %s (%s) failed attempt %d/%d: %s",
                job_id,
                job.type,
                job.attempts,
                job.max_attempts,
                error,
            )
            self._on_failure(job, started_at, error)

    def _on_failure(self, job: Job, started_at: datetime, error: str) -> None:
        if job.attempts >= job.max_attempts:
            self._repo.move_to_dlq(
                job.id,
                attempt=job.attempts,
                started_at=started_at,
                error=error,
                worker_id=self.worker_id,
            )
            JOBS_FAILED.labels(job_type=job.type, reason="max_attempts").inc()
            logger.warning("Job %s dead after %d attempts", job.id, job.attempts)
        else:
            delay = backoff_seconds(job.attempts)
            run_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            self._repo.reschedule(job.id, run_at, error=error)
            self._queue.enqueue(job.id, job.priority, run_at=run_at.timestamp())
            logger.info(
                "Job %s rescheduled in %.1fs (attempt %d/%d)",
                job.id,
                delay,
                job.attempts,
                job.max_attempts,
            )


# ── entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    import worker.handlers  # noqa: F401 — side-effect: registers all handlers

    from core.config import settings
    from core.registry import registry

    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(settings.database_url)
    redis_client = redis_lib.from_url(settings.effective_redis_url)
    Worker(conn, redis_client, registry).run()


if __name__ == "__main__":
    main()
