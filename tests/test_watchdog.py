"""Integration tests for the Watchdog — uses real Postgres and Redis."""
import os
import time
import uuid

import psycopg2
import pytest
import redis as redis_lib

from core.models import CreateJobRequest, JobStatus, Priority
from core.queue import RedisQueue
from core.repository import JobRepository
from watchdog.watchdog import Watchdog


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
def queue(redis_client):
    return RedisQueue(redis_client)


@pytest.fixture()
def watchdog(conn, redis_client):
    # Use a very short threshold so tests can set "stale" heartbeats easily
    return Watchdog(conn, redis_client, threshold_seconds=5)


def make_running_job(repo: JobRepository, worker_id: str) -> object:
    job = repo.create(
        CreateJobRequest(type="test.noop", payload={}, priority=Priority.NORMAL)
    )
    return repo.update_status(
        job.id, JobStatus.RUNNING, worker_id=worker_id, attempts_increment=1
    )


class TestStaleness:
    def test_fresh_worker_not_reclaimed(self, watchdog, repo, queue, redis_client):
        worker_id = f"worker-{uuid.uuid4()}"
        queue.register_heartbeat(worker_id)  # fresh — just now
        make_running_job(repo, worker_id)

        reclaimed = watchdog._tick()
        assert reclaimed == 0
        assert repo.list(status=JobStatus.RUNNING)[0].worker_id == worker_id

    def test_stale_worker_jobs_reclaimed(self, watchdog, repo, queue, redis_client):
        worker_id = f"worker-{uuid.uuid4()}"
        # Write a heartbeat timestamp 60s in the past (stale for our 5s threshold)
        redis_client.set(f"worker:heartbeat:{worker_id}", time.time() - 60)
        job = make_running_job(repo, worker_id)

        reclaimed = watchdog._tick()
        assert reclaimed == 1

        updated = repo.get_by_id(job.id)
        assert updated.status == JobStatus.PENDING
        assert updated.worker_id is None

    def test_reclaimed_job_re_enqueued_to_redis(self, watchdog, repo, queue, redis_client):
        worker_id = f"worker-{uuid.uuid4()}"
        redis_client.set(f"worker:heartbeat:{worker_id}", time.time() - 60)
        make_running_job(repo, worker_id)

        watchdog._tick()

        assert redis_client.llen("queue:normal") == 1

    def test_stale_heartbeat_removed_after_reclaim(self, watchdog, repo, redis_client):
        worker_id = f"worker-{uuid.uuid4()}"
        redis_client.set(f"worker:heartbeat:{worker_id}", time.time() - 60)
        make_running_job(repo, worker_id)

        watchdog._tick()
        # Second tick should not report this worker again
        reclaimed_again = watchdog._tick()
        assert reclaimed_again == 0

    def test_done_jobs_not_reclaimed(self, watchdog, repo, queue, redis_client):
        worker_id = f"worker-{uuid.uuid4()}"
        redis_client.set(f"worker:heartbeat:{worker_id}", time.time() - 60)

        job = repo.create(
            CreateJobRequest(type="test.noop", payload={}, priority=Priority.NORMAL)
        )
        repo.update_status(job.id, JobStatus.RUNNING, worker_id=worker_id, attempts_increment=1)
        repo.update_status(job.id, JobStatus.DONE, result={})

        reclaimed = watchdog._tick()
        assert reclaimed == 0
        assert repo.get_by_id(job.id).status == JobStatus.DONE

    def test_multiple_jobs_from_same_stale_worker(self, watchdog, repo, queue, redis_client):
        worker_id = f"worker-{uuid.uuid4()}"
        redis_client.set(f"worker:heartbeat:{worker_id}", time.time() - 60)
        make_running_job(repo, worker_id)
        make_running_job(repo, worker_id)
        make_running_job(repo, worker_id)

        reclaimed = watchdog._tick()
        assert reclaimed == 3
        assert redis_client.llen("queue:normal") == 3

    def test_tick_returns_zero_with_no_stale_workers(self, watchdog):
        assert watchdog._tick() == 0
