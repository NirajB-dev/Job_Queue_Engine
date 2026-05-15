"""Integration tests for RedisQueue — runs against a real Redis instance."""
import os
import time
import uuid

import pytest
import redis

from core.models import Priority
from core.queue import RedisQueue


@pytest.fixture(scope="session")
def redis_client():
    client = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
    yield client
    client.flushdb()


@pytest.fixture(autouse=True)
def clean_redis(redis_client):
    redis_client.flushdb()
    yield
    redis_client.flushdb()


@pytest.fixture()
def queue(redis_client):
    return RedisQueue(redis_client)


class TestEnqueue:
    def test_immediate_job_lands_in_list(self, queue, redis_client):
        job_id = uuid.uuid4()
        queue.enqueue(job_id, Priority.NORMAL, run_at=time.time() - 1)
        assert redis_client.llen("queue:normal") == 1

    def test_future_job_lands_in_zset(self, queue, redis_client):
        job_id = uuid.uuid4()
        queue.enqueue(job_id, Priority.NORMAL, run_at=time.time() + 60)
        assert redis_client.zcard("delayed:jobs") == 1
        assert redis_client.llen("queue:normal") == 0

    def test_priority_routing(self, queue, redis_client):
        queue.enqueue(uuid.uuid4(), Priority.HIGH, run_at=time.time() - 1)
        queue.enqueue(uuid.uuid4(), Priority.LOW, run_at=time.time() - 1)
        assert redis_client.llen("queue:high") == 1
        assert redis_client.llen("queue:low") == 1
        assert redis_client.llen("queue:normal") == 0


class TestDequeueBlocking:
    def test_returns_none_on_empty_queues(self, queue):
        result = queue.dequeue_blocking(timeout=1)
        assert result is None

    def test_dequeues_job(self, queue):
        job_id = uuid.uuid4()
        queue.enqueue(job_id, Priority.NORMAL, run_at=time.time() - 1)
        result = queue.dequeue_blocking(timeout=1)
        assert result is not None
        assert result.job_id == job_id
        assert result.priority == Priority.NORMAL

    def test_strict_priority_ordering(self, queue):
        low_id = uuid.uuid4()
        high_id = uuid.uuid4()
        # Enqueue low first to confirm ordering is by priority, not insertion
        queue.enqueue(low_id, Priority.LOW, run_at=time.time() - 1)
        queue.enqueue(high_id, Priority.HIGH, run_at=time.time() - 1)
        first = queue.dequeue_blocking(timeout=1)
        assert first.job_id == high_id
        assert first.priority == Priority.HIGH


class TestPromoteDelayed:
    def test_promotes_due_jobs(self, queue, redis_client):
        job_id = uuid.uuid4()
        queue.enqueue(job_id, Priority.NORMAL, run_at=time.time() - 1)
        # Enqueue directly to delayed ZSET with past timestamp
        redis_client.flushdb()
        member = f"{job_id}:{int(Priority.NORMAL)}"
        redis_client.zadd("delayed:jobs", {member: time.time() - 5})

        promoted = queue.promote_delayed()
        assert promoted == 1
        assert redis_client.llen("queue:normal") == 1
        assert redis_client.zcard("delayed:jobs") == 0

    def test_does_not_promote_future_jobs(self, queue, redis_client):
        job_id = uuid.uuid4()
        member = f"{job_id}:{int(Priority.HIGH)}"
        redis_client.zadd("delayed:jobs", {member: time.time() + 60})

        promoted = queue.promote_delayed()
        assert promoted == 0
        assert redis_client.llen("queue:high") == 0

    def test_returns_zero_when_empty(self, queue):
        assert queue.promote_delayed() == 0


class TestHeartbeats:
    def test_register_and_detect_fresh_worker(self, queue):
        queue.register_heartbeat("worker-1")
        stale = queue.get_stale_workers(threshold_seconds=60)
        assert "worker-1" not in stale

    def test_detect_stale_worker(self, queue, redis_client):
        redis_client.set("worker:heartbeat:old-worker", time.time() - 120)
        stale = queue.get_stale_workers(threshold_seconds=60)
        assert "old-worker" in stale

    def test_deregister_removes_worker(self, queue):
        queue.register_heartbeat("worker-2")
        queue.deregister_worker("worker-2")
        stale = queue.get_stale_workers(threshold_seconds=0)
        assert "worker-2" not in stale


class TestMetrics:
    def test_queue_depths(self, queue):
        queue.enqueue(uuid.uuid4(), Priority.HIGH, run_at=time.time() - 1)
        queue.enqueue(uuid.uuid4(), Priority.HIGH, run_at=time.time() - 1)
        queue.enqueue(uuid.uuid4(), Priority.LOW, run_at=time.time() - 1)
        depths = queue.queue_depths()
        assert depths[Priority.HIGH] == 2
        assert depths[Priority.NORMAL] == 0
        assert depths[Priority.LOW] == 1

    def test_delayed_count(self, queue):
        queue.enqueue(uuid.uuid4(), Priority.NORMAL, run_at=time.time() + 60)
        queue.enqueue(uuid.uuid4(), Priority.NORMAL, run_at=time.time() + 60)
        assert queue.delayed_count() == 2
