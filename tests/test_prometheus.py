"""Integration tests for the Prometheus scrape endpoint."""
from __future__ import annotations

import os

import psycopg2
import pytest
import redis as redis_lib
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture(scope="session")
def db_conn():
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    yield c
    c.close()


@pytest.fixture(scope="session")
def redis_client():
    return redis_lib.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))


@pytest.fixture(autouse=True)
def clean(db_conn, redis_client):
    yield
    db_conn.rollback()
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM job_attempts")
        cur.execute("DELETE FROM jobs")
    db_conn.commit()
    redis_client.flushdb()


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
def headers():
    return {"X-API-Key": os.environ.get("API_KEY", "test-api-key")}


class TestPrometheusEndpoint:
    def test_returns_200(self, client):
        r = client.get("/prometheus")
        assert r.status_code == 200

    def test_content_type_is_text_plain(self, client):
        r = client.get("/prometheus")
        assert "text/plain" in r.headers["content-type"]

    def test_no_auth_required(self, client):
        r = client.get("/prometheus")
        assert r.status_code == 200

    def test_contains_queue_depth_metric(self, client):
        r = client.get("/prometheus")
        assert "job_queue_depth" in r.text

    def test_contains_delayed_metric(self, client):
        r = client.get("/prometheus")
        assert "job_queue_delayed" in r.text

    def test_contains_jobs_by_status_metric(self, client):
        r = client.get("/prometheus")
        assert "job_queue_jobs_by_status" in r.text

    def test_queue_depth_updates_after_enqueue(self, client, headers, redis_client):
        client.post("/jobs", json={"type": "test.echo", "payload": {}, "priority": 0}, headers=headers)
        r = client.get("/prometheus")
        assert 'job_queue_depth{priority="high"} 1.0' in r.text

    def test_delayed_count_updates(self, client, redis_client):
        import time
        redis_client.zadd("delayed:jobs", {f"some-uuid:{1}": time.time() + 3600})
        r = client.get("/prometheus")
        assert "job_queue_delayed 1.0" in r.text

    def test_contains_worker_counter_definitions(self, client):
        r = client.get("/prometheus")
        assert "job_worker_jobs_processed_total" in r.text
        assert "job_worker_jobs_failed_total" in r.text
        assert "job_worker_job_duration_seconds" in r.text
