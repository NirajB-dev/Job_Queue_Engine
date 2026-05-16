from __future__ import annotations

import time

from fastapi import APIRouter, Depends

from api.dependencies import QueueDepend
from api.middleware.auth import require_api_key

router = APIRouter(prefix="/workers", tags=["workers"], dependencies=[Depends(require_api_key)])

_HEARTBEAT_PREFIX = "worker:heartbeat:"


@router.get("")
def list_workers(queue: QueueDepend) -> list[dict]:
    """Return all workers that have a live heartbeat key in Redis."""
    redis_client = queue._r
    workers = []
    for key in redis_client.scan_iter(f"{_HEARTBEAT_PREFIX}*"):
        raw = redis_client.get(key)
        if raw is None:
            continue
        key_str = key.decode() if isinstance(key, bytes) else key
        worker_id = key_str.removeprefix(_HEARTBEAT_PREFIX)
        last_seen = float(raw)
        workers.append(
            {
                "worker_id": worker_id,
                "last_heartbeat": last_seen,
                "age_seconds": round(time.time() - last_seen, 1),
            }
        )
    return sorted(workers, key=lambda w: w["last_heartbeat"], reverse=True)
