from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

from core.models import CreateJobRequest, Job, JobAttempt, Priority

psycopg2.extras.register_uuid()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class JobRepository:
    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self._conn = conn

    # ── writes ────────────────────────────────────────────────────────────────

    def create(self, req: CreateJobRequest) -> Job:
        run_at = req.run_at or _utcnow()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO jobs
                        (type, payload, priority, max_attempts, idempotency_key,
                         scheduled_at, run_at)
                    VALUES
                        (%(type)s, %(payload)s, %(priority)s, %(max_attempts)s,
                         %(idempotency_key)s, now(), %(run_at)s)
                    RETURNING *
                    """,
                    {
                        "type": req.type,
                        "payload": psycopg2.extras.Json(req.payload),
                        "priority": int(req.priority),
                        "max_attempts": req.max_attempts,
                        "idempotency_key": req.idempotency_key,
                        "run_at": run_at,
                    },
                )
                self._conn.commit()
                return Job.model_validate(dict(cur.fetchone()))
        except Exception:
            self._conn.rollback()
            raise

    def update_status(
        self,
        job_id: uuid.UUID,
        status: str,
        *,
        worker_id: str | None = None,
        error: str | None = None,
        result: dict[str, Any] | None = None,
        attempts_increment: int = 0,
    ) -> Job:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status              = %(status)s,
                    worker_id           = COALESCE(%(worker_id)s, worker_id),
                    error               = COALESCE(%(error)s, error),
                    result              = COALESCE(%(result)s, result),
                    attempts            = attempts + %(attempts_increment)s,
                    started_at          = CASE WHEN %(status)s = 'running'
                                              THEN now() ELSE started_at END,
                    completed_at        = CASE WHEN %(status)s IN ('done','failed','dead')
                                              THEN now() ELSE completed_at END
                WHERE id = %(job_id)s
                RETURNING *
                """,
                {
                    "status": status,
                    "worker_id": worker_id,
                    "error": error,
                    "result": psycopg2.extras.Json(result) if result else None,
                    "attempts_increment": attempts_increment,
                    "job_id": job_id,
                },
            )
            self._conn.commit()
            return Job.model_validate(dict(cur.fetchone()))

    def reschedule(
        self, job_id: uuid.UUID, run_at: datetime, error: str | None = None
    ) -> None:
        """Push run_at forward for a delayed retry and set status back to pending."""
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE jobs SET status = 'pending', run_at = %s,
                   error = COALESCE(%s, error) WHERE id = %s""",
                (run_at, error, job_id),
            )
            self._conn.commit()

    def move_to_dlq(
        self,
        job_id: uuid.UUID,
        attempt: int,
        started_at: datetime,
        error: str,
        worker_id: str,
    ) -> None:
        """Mark job dead and record the final failed attempt."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET status = 'dead', completed_at = now(),
                    error = %s
                WHERE id = %s
                """,
                (error, job_id),
            )
            cur.execute(
                """
                INSERT INTO job_attempts
                    (job_id, attempt, started_at, failed_at, error, worker_id)
                VALUES (%s, %s, %s, now(), %s, %s)
                """,
                (job_id, attempt, started_at, error, worker_id),
            )
            self._conn.commit()

    def requeue(self, job_id: uuid.UUID) -> Job:
        """Reset a dead job to pending so it can be re-enqueued."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status       = 'pending',
                    attempts     = 0,
                    error        = NULL,
                    worker_id    = NULL,
                    completed_at = NULL,
                    run_at       = now()
                WHERE id = %s AND status = 'dead'
                RETURNING *
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Job {job_id} is not in dead status")
            self._conn.commit()
            return Job.model_validate(dict(row))

    # ── reads ─────────────────────────────────────────────────────────────────

    def get_by_id(self, job_id: uuid.UUID) -> Job | None:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
            return Job.model_validate(dict(row)) if row else None

    def list(
        self,
        *,
        status: str | None = None,
        job_type: str | None = None,
        priority: Priority | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        filters: list[str] = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if status is not None:
            filters.append("status = %(status)s")
            params["status"] = status
        if job_type is not None:
            filters.append("type = %(job_type)s")
            params["job_type"] = job_type
        if priority is not None:
            filters.append("priority = %(priority)s")
            params["priority"] = int(priority)

        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM jobs {where} ORDER BY run_at DESC "
                f"LIMIT %(limit)s OFFSET %(offset)s",
                params,
            )
            return [Job.model_validate(dict(r)) for r in cur.fetchall()]

    def get_attempts(self, job_id: uuid.UUID) -> list[JobAttempt]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM job_attempts WHERE job_id = %s ORDER BY attempt",
                (job_id,),
            )
            return [JobAttempt.model_validate(dict(r)) for r in cur.fetchall()]

    def claim_stale_jobs(self, stale_worker_id: str) -> list[Job]:
        """Return jobs held by a crashed worker and reset them to pending."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE jobs SET status = 'pending', worker_id = NULL
                WHERE worker_id = %s AND status = 'running'
                RETURNING *
                """,
                (stale_worker_id,),
            )
            rows = cur.fetchall()
            self._conn.commit()
            return [Job.model_validate(dict(r)) for r in rows]
