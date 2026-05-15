from __future__ import annotations

import time
import uuid
from typing import NamedTuple

import redis

from core.models import Priority

# Redis key constants
_QUEUE_KEYS: dict[Priority, str] = {
    Priority.HIGH: "queue:high",
    Priority.NORMAL: "queue:normal",
    Priority.LOW: "queue:low",
}
_DEQUEUE_ORDER = ["queue:high", "queue:normal", "queue:low"]
_DELAYED_ZSET = "delayed:jobs"
_HEARTBEAT_PREFIX = "worker:heartbeat:"


class DequeuedJob(NamedTuple):
    job_id: uuid.UUID
    priority: Priority


class RedisQueue:
    def __init__(self, client: redis.Redis) -> None:
        self._r = client

    # ── enqueueing ────────────────────────────────────────────────────────────

    def enqueue(self, job_id: uuid.UUID, priority: Priority, run_at: float) -> None:
        """Push a job onto the appropriate queue.

        If run_at is in the future the job goes into the delayed ZSET;
        the scheduler promotes it to the live list when its time arrives.
        """
        member = f"{job_id}:{int(priority)}"
        if run_at <= time.time():
            self._r.rpush(_QUEUE_KEYS[priority], str(job_id))
        else:
            self._r.zadd(_DELAYED_ZSET, {member: run_at})

    # ── dequeueing ────────────────────────────────────────────────────────────

    def dequeue_blocking(self, timeout: int = 5) -> DequeuedJob | None:
        """Block up to *timeout* seconds for the next job.

        BLPOP on [high, normal, low] gives strict priority ordering in a
        single atomic call — no polling, no starvation logic needed for
        HIGH and NORMAL.
        """
        result = self._r.blpop(_DEQUEUE_ORDER, timeout=timeout)
        if result is None:
            return None
        key, value = result
        job_id = uuid.UUID(value.decode() if isinstance(value, bytes) else value)
        key_str = key.decode() if isinstance(key, bytes) else key
        priority = next(p for p, k in _QUEUE_KEYS.items() if k == key_str)
        return DequeuedJob(job_id=job_id, priority=priority)

    # ── scheduler: promote delayed jobs ───────────────────────────────────────

    def promote_delayed(self) -> int:
        """Move all delayed jobs whose run_at <= now into their priority list.

        Uses a pipeline so each (RPUSH + ZREM) pair is sent in one round-trip.
        Returns the number of jobs promoted.
        """
        due = self._r.zrangebyscore(_DELAYED_ZSET, 0, time.time())
        if not due:
            return 0

        pipe = self._r.pipeline()
        for member in due:
            raw = member.decode() if isinstance(member, bytes) else member
            job_id_str, priority_int = raw.rsplit(":", 1)
            priority = Priority(int(priority_int))
            pipe.rpush(_QUEUE_KEYS[priority], job_id_str)
            pipe.zrem(_DELAYED_ZSET, member)
        pipe.execute()
        return len(due)

    # ── worker heartbeats ─────────────────────────────────────────────────────

    def register_heartbeat(self, worker_id: str) -> None:
        """Refresh the heartbeat timestamp for a worker (call every ~15 s)."""
        self._r.set(f"{_HEARTBEAT_PREFIX}{worker_id}", time.time())

    def deregister_worker(self, worker_id: str) -> None:
        """Remove heartbeat key on clean worker shutdown."""
        self._r.delete(f"{_HEARTBEAT_PREFIX}{worker_id}")

    def get_stale_workers(self, threshold_seconds: int = 60) -> list[str]:
        """Return worker IDs whose last heartbeat is older than threshold."""
        cutoff = time.time() - threshold_seconds
        stale: list[str] = []
        for key in self._r.scan_iter(f"{_HEARTBEAT_PREFIX}*"):
            raw = self._r.get(key)
            if raw is None:
                continue
            last_seen = float(raw)
            if last_seen < cutoff:
                worker_id = (
                    key.decode() if isinstance(key, bytes) else key
                ).removeprefix(_HEARTBEAT_PREFIX)
                stale.append(worker_id)
        return stale

    # ── metrics helpers ───────────────────────────────────────────────────────

    def queue_depths(self) -> dict[str, int]:
        return {name: self._r.llen(key) for name, key in _QUEUE_KEYS.items()}

    def delayed_count(self) -> int:
        return self._r.zcard(_DELAYED_ZSET)
