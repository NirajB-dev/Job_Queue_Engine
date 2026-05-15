"""Integration tests for the Scheduler — uses a real Redis instance."""
import os
import time
import uuid

import pytest
import redis as redis_lib

from core.models import Priority
from scheduler.scheduler import Scheduler


@pytest.fixture(scope="session")
def redis_client():
    return redis_lib.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))


@pytest.fixture(autouse=True)
def clean_redis(redis_client):
    yield
    redis_client.flushdb()


@pytest.fixture()
def scheduler(redis_client):
    return Scheduler(redis_client)


def _add_delayed(redis_client, priority: Priority, score: float) -> str:
    job_id = str(uuid.uuid4())
    member = f"{job_id}:{int(priority)}"
    redis_client.zadd("delayed:jobs", {member: score})
    return job_id


class TestTick:
    def test_promotes_overdue_job(self, scheduler, redis_client):
        _add_delayed(redis_client, Priority.NORMAL, time.time() - 5)
        promoted = scheduler._tick()
        assert promoted == 1
        assert redis_client.llen("queue:normal") == 1
        assert redis_client.zcard("delayed:jobs") == 0

    def test_promotes_to_correct_priority_queue(self, scheduler, redis_client):
        _add_delayed(redis_client, Priority.HIGH, time.time() - 1)
        _add_delayed(redis_client, Priority.LOW, time.time() - 1)
        scheduler._tick()
        assert redis_client.llen("queue:high") == 1
        assert redis_client.llen("queue:low") == 1
        assert redis_client.llen("queue:normal") == 0

    def test_does_not_promote_future_jobs(self, scheduler, redis_client):
        _add_delayed(redis_client, Priority.NORMAL, time.time() + 60)
        promoted = scheduler._tick()
        assert promoted == 0
        assert redis_client.llen("queue:normal") == 0

    def test_returns_zero_when_empty(self, scheduler):
        assert scheduler._tick() == 0

    def test_partial_promotion(self, scheduler, redis_client):
        """Only due jobs are promoted; future jobs stay in ZSET."""
        _add_delayed(redis_client, Priority.NORMAL, time.time() - 1)
        _add_delayed(redis_client, Priority.HIGH, time.time() + 60)
        promoted = scheduler._tick()
        assert promoted == 1
        assert redis_client.llen("queue:normal") == 1
        assert redis_client.zcard("delayed:jobs") == 1  # future job remains

    def test_multiple_ticks_idempotent(self, scheduler, redis_client):
        """A second tick on already-promoted jobs does nothing."""
        _add_delayed(redis_client, Priority.NORMAL, time.time() - 1)
        scheduler._tick()
        promoted_again = scheduler._tick()
        assert promoted_again == 0
        assert redis_client.llen("queue:normal") == 1
