"""Integration tests for the Worker — uses real Postgres and Redis."""
import os
import uuid

import psycopg2
import pytest
import redis as redis_lib

from core.models import CreateJobRequest, JobStatus, Priority
from core.queue import RedisQueue
from core.registry import HandlerRegistry
from core.repository import JobRepository
from worker.worker import Worker


@pytest.fixture(scope="session")
def conn():
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    yield c
    c.close()


@pytest.fixture(scope="session")
def redis_client():
    return redis_lib.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))


@pytest.fixture(autouse=True)
def clean(conn, redis_client):
    yield
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM job_attempts")
        cur.execute("DELETE FROM jobs")
    conn.commit()
    redis_client.flushdb()


@pytest.fixture()
def repo(conn):
    return JobRepository(conn)


@pytest.fixture()
def test_registry():
    return HandlerRegistry()


@pytest.fixture()
def worker(conn, redis_client, test_registry):
    return Worker(conn, redis_client, test_registry)


def make_job(repo: JobRepository, **kwargs):
    return repo.create(
        CreateJobRequest(
            type=kwargs.get("type", "test.noop"),
            payload=kwargs.get("payload", {}),
            priority=kwargs.get("priority", Priority.NORMAL),
            max_attempts=kwargs.get("max_attempts", 3),
        )
    )


class TestSuccessPath:
    def test_job_marked_done_with_result(self, worker, repo, test_registry):
        @test_registry.register("test.echo")
        def handler(job):
            return {"echoed": job.payload}

        job = make_job(repo, type="test.echo", payload={"x": 42})
        worker._process(job.id)

        updated = repo.get_by_id(job.id)
        assert updated.status == JobStatus.DONE
        assert updated.result == {"echoed": {"x": 42}}
        assert updated.worker_id == worker.worker_id
        assert updated.attempts == 1
        assert updated.completed_at is not None

    def test_handler_returning_none_stores_empty_dict(self, worker, repo, test_registry):
        @test_registry.register("test.none")
        def handler(job):
            return None

        job = make_job(repo, type="test.none")
        worker._process(job.id)
        assert repo.get_by_id(job.id).status == JobStatus.DONE


class TestRetryPath:
    def test_failed_job_rescheduled_when_attempts_remaining(
        self, worker, repo, test_registry, redis_client
    ):
        @test_registry.register("test.fail_once")
        def handler(job):
            raise RuntimeError("transient error")

        job = make_job(repo, type="test.fail_once", max_attempts=3)
        worker._process(job.id)

        updated = repo.get_by_id(job.id)
        assert updated.status == JobStatus.PENDING  # rescheduled, not dead
        assert updated.attempts == 1
        assert updated.error == "transient error"
        assert RedisQueue(redis_client).delayed_count() == 1  # sitting in ZSET

    def test_job_dead_at_max_attempts(self, worker, repo, test_registry):
        @test_registry.register("test.always_fail")
        def handler(job):
            raise RuntimeError("always fails")

        job = make_job(repo, type="test.always_fail", max_attempts=1)
        worker._process(job.id)

        updated = repo.get_by_id(job.id)
        assert updated.status == JobStatus.DEAD
        attempts = repo.get_attempts(job.id)
        assert len(attempts) == 1
        assert attempts[0].error == "always fails"
        assert attempts[0].worker_id == worker.worker_id

    def test_three_failures_exhaust_attempts(self, worker, repo, test_registry, redis_client):
        """Simulate three successive process calls to verify DLQ promotion."""

        @test_registry.register("test.three_fails")
        def handler(job):
            raise RuntimeError("boom")

        job = make_job(repo, type="test.three_fails", max_attempts=3)

        # First two failures → reschedule
        for _ in range(2):
            worker._process(job.id)
            job = repo.get_by_id(job.id)
            assert job.status == JobStatus.PENDING
            # Manually promote delayed job back to queue for next iteration
            redis_client.flushdb()

        # Third failure → dead
        worker._process(job.id)
        final = repo.get_by_id(job.id)
        assert final.status == JobStatus.DEAD
        assert final.attempts == 3


class TestUnknownType:
    def test_unknown_type_goes_to_dlq_immediately(self, worker, repo):
        job = make_job(repo, type="type.does.not.exist")
        worker._process(job.id)

        updated = repo.get_by_id(job.id)
        assert updated.status == JobStatus.DEAD
        assert "No handler" in updated.error


class TestMissingJob:
    def test_missing_job_id_is_silently_skipped(self, worker):
        worker._process(uuid.uuid4())  # must not raise
