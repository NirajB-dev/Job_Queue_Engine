from __future__ import annotations

from contextlib import asynccontextmanager

import psycopg2
import redis as redis_lib
from fastapi import FastAPI

from api.routers import jobs, metrics, workers
from core.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_conn = psycopg2.connect(settings.database_url)
    app.state.redis_client = redis_lib.from_url(settings.effective_redis_url)
    yield
    app.state.db_conn.close()


app = FastAPI(title="Job Queue Engine", lifespan=lifespan)

app.include_router(jobs.router)
app.include_router(workers.router)
app.include_router(metrics.router)


@app.get("/health")
def health():
    return {"status": "ok"}
