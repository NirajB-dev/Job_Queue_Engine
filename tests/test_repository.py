"""Integration tests for JobRepository — runs against a real Postgres."""
import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from core.models import CreateJobRequest, JobStatus, Priority
from core.repository import JobRepository


@pytest.fixture(scope="session")
def conn():
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    yield c
    c.close()


@pytest.fixture(autouse=True)
def clean_tables(conn):
    yield
    with conn.cursor() as cur:
        cur.execute("DELETE FROM job_attempts")
        cur.execute("DELETE FROM jobs")
    conn.commit()


@pytest.fixture()
def repo(conn):
    return JobRepository(conn)


def make_req(**kwargs) -> CreateJobRequest:
    return CreateJobRequest(
        type=kwargs.get("type", "test.noop"),
        payload=kwargs.get("payload", {"x": 1}),
        priority=kwargs.get("priority", Priority.NORMAL),
        max_attempts=kwargs.get("max_attempts", 3),
        idempotency_key=kwargs.get("idempotency_key", None),
        run_at=kwargs.get("run_at", None),
    )


class TestCreate:
    def test_basic_create(self, repo):
        job = repo.create(make_req())
        assert job.status == JobStatus.PENDING
        assert job.attempts == 0
        assert job.priority == Priority.NORMAL

    def test_idempotency_key_unique(self, repo):
        repo.create(make_req(idempotency_key="key-1"))
        with pytest.raises(Exception):
            repo.create(make_req(idempotency_key="key-1"))

    def test_future_run_at(self, repo):
        future = datetime.now(timezone.utc) + timedelta(minutes=5)
        job = repo.create(make_req(run_at=future))
        assert job.run_at >= future.replace(microsecond=0)


class TestGetById:
    def test_existing_job(self, repo):
        created = repo.create(make_req())
        fetched = repo.get_by_id(created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_missing_job_returns_none(self, repo):
        assert repo.get_by_id(uuid.uuid4()) is None


class TestList:
    def test_filter_by_status(self, repo):
        repo.create(make_req())
        repo.create(make_req())
        results = repo.list(status=JobStatus.PENDING)
        assert len(results) == 2

    def test_filter_by_priority(self, repo):
        repo.create(make_req(priority=Priority.HIGH))
        repo.create(make_req(priority=Priority.LOW))
        high = repo.list(priority=Priority.HIGH)
        assert all(j.priority == Priority.HIGH for j in high)
        assert len(high) == 1

    def test_limit_offset(self, repo):
        for _ in range(5):
            repo.create(make_req())
        page1 = repo.list(limit=2, offset=0)
        page2 = repo.list(limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert {j.id for j in page1}.isdisjoint({j.id for j in page2})


class TestUpdateStatus:
    def test_mark_running(self, repo):
        job = repo.create(make_req())
        updated = repo.update_status(
            job.id, JobStatus.RUNNING, worker_id="w-1", attempts_increment=1
        )
        assert updated.status == JobStatus.RUNNING
        assert updated.worker_id == "w-1"
        assert updated.started_at is not None
        assert updated.attempts == 1

    def test_mark_done_with_result(self, repo):
        job = repo.create(make_req())
        updated = repo.update_status(
            job.id, JobStatus.DONE, result={"output": "ok"}
        )
        assert updated.status == JobStatus.DONE
        assert updated.completed_at is not None
        assert updated.result == {"output": "ok"}


class TestMoveToDeadLetter:
    def test_dead_job_and_attempt_recorded(self, repo):
        job = repo.create(make_req())
        started = datetime.now(timezone.utc)
        repo.move_to_dlq(job.id, attempt=3, started_at=started,
                         error="handler blew up", worker_id="w-1")
        dead = repo.get_by_id(job.id)
        assert dead.status == JobStatus.DEAD
        attempts = repo.get_attempts(job.id)
        assert len(attempts) == 1
        assert attempts[0].error == "handler blew up"


class TestRequeue:
    def test_requeue_dead_job(self, repo):
        job = repo.create(make_req())
        started = datetime.now(timezone.utc)
        repo.move_to_dlq(job.id, attempt=3, started_at=started,
                         error="boom", worker_id="w-1")
        requeued = repo.requeue(job.id)
        assert requeued.status == JobStatus.PENDING
        assert requeued.attempts == 0
        assert requeued.error is None

    def test_requeue_non_dead_raises(self, repo):
        job = repo.create(make_req())
        with pytest.raises(ValueError):
            repo.requeue(job.id)


class TestClaimStaleJobs:
    def test_reclaim_running_jobs_from_crashed_worker(self, repo):
        job = repo.create(make_req())
        repo.update_status(job.id, JobStatus.RUNNING, worker_id="dead-worker")
        reclaimed = repo.claim_stale_jobs("dead-worker")
        assert len(reclaimed) == 1
        assert reclaimed[0].status == JobStatus.PENDING
        assert reclaimed[0].worker_id is None

    def test_does_not_reclaim_done_jobs(self, repo):
        job = repo.create(make_req())
        repo.update_status(job.id, JobStatus.RUNNING, worker_id="dead-worker")
        repo.update_status(job.id, JobStatus.DONE)
        reclaimed = repo.claim_stale_jobs("dead-worker")
        assert len(reclaimed) == 0
