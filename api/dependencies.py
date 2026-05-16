from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request

from core.queue import RedisQueue
from core.repository import JobRepository


def get_repo(request: Request) -> JobRepository:
    return JobRepository(request.app.state.db_conn)


def get_queue(request: Request) -> RedisQueue:
    return RedisQueue(request.app.state.redis_client)


RepoDepend = Annotated[JobRepository, Depends(get_repo)]
QueueDepend = Annotated[RedisQueue, Depends(get_queue)]
