"""Integration tests for the FastAPI control plane."""
from __future__ import annotations

import os
import uuid

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


# ── /health ───────────────────────────────────────────────────────────────────

class TestHealth:
    def test_health_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


# ── auth ──────────────────────────────────────────────────────────────────────

class TestAuth:
    def test_missing_key_rejected(self, client):
        r = client.get("/jobs")
        assert r.status_code == 401

    def test_wrong_key_rejected(self, client):
        r = client.get("/jobs", headers={"X-API-Key": "wrong"})
        assert r.status_code == 401

    def test_correct_key_accepted(self, client, headers):
        r = client.get("/jobs", headers=headers)
        assert r.status_code == 200


# ── POST /jobs ────────────────────────────────────────────────────────────────

class TestCreateJob:
    def test_creates_job_returns_201(self, client, headers, redis_client):
        r = client.post("/jobs", json={"type": "test.echo", "payload": {"x": 1}}, headers=headers)
        assert r.status_code == 201
        data = r.json()
        assert data["type"] == "test.echo"
        assert data["status"] == "pending"
        assert data["priority"] == 1  # NORMAL

    def test_job_enqueued_to_redis(self, client, headers, redis_client):
        client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers)
        assert redis_client.llen("queue:normal") == 1

    def test_high_priority_goes_to_high_queue(self, client, headers, redis_client):
        client.post("/jobs", json={"type": "test.echo", "payload": {}, "priority": 0}, headers=headers)
        assert redis_client.llen("queue:high") == 1

    def test_idempotency_key_dedup(self, client, headers):
        key = str(uuid.uuid4())
        r1 = client.post("/jobs", json={"type": "test.echo", "payload": {}, "idempotency_key": key}, headers=headers)
        r2 = client.post("/jobs", json={"type": "test.echo", "payload": {}, "idempotency_key": key}, headers=headers)
        assert r1.status_code == 201
        assert r2.status_code == 409

    def test_missing_type_rejected(self, client, headers):
        r = client.post("/jobs", json={"payload": {}}, headers=headers)
        assert r.status_code == 422


# ── GET /jobs ─────────────────────────────────────────────────────────────────

class TestListJobs:
    def test_returns_empty_list(self, client, headers):
        r = client.get("/jobs", headers=headers)
        assert r.status_code == 200
        assert r.json() == []

    def test_returns_created_job(self, client, headers):
        client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers)
        r = client.get("/jobs", headers=headers)
        assert len(r.json()) == 1

    def test_filter_by_status(self, client, headers):
        client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers)
        r = client.get("/jobs?status=pending", headers=headers)
        assert all(j["status"] == "pending" for j in r.json())

    def test_filter_by_type(self, client, headers):
        client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers)
        client.post("/jobs", json={"type": "test.other", "payload": {}}, headers=headers)
        r = client.get("/jobs?type=test.echo", headers=headers)
        assert all(j["type"] == "test.echo" for j in r.json())
        assert len(r.json()) == 1

    def test_pagination(self, client, headers):
        for _ in range(5):
            client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers)
        r = client.get("/jobs?limit=2&offset=0", headers=headers)
        assert len(r.json()) == 2


# ── GET /jobs/{id} ────────────────────────────────────────────────────────────

class TestGetJob:
    def test_returns_job(self, client, headers):
        created = client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers).json()
        r = client.get(f"/jobs/{created['id']}", headers=headers)
        assert r.status_code == 200
        assert r.json()["id"] == created["id"]

    def test_404_for_missing(self, client, headers):
        r = client.get(f"/jobs/{uuid.uuid4()}", headers=headers)
        assert r.status_code == 404


# ── POST /jobs/{id}/cancel ────────────────────────────────────────────────────

class TestCancelJob:
    def test_cancels_pending_job(self, client, headers):
        job = client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers).json()
        r = client.post(f"/jobs/{job['id']}/cancel", headers=headers)
        assert r.status_code == 200
        assert r.json()["status"] == "dead"

    def test_cannot_cancel_non_pending(self, client, headers):
        job = client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers).json()
        client.post(f"/jobs/{job['id']}/cancel", headers=headers)
        r = client.post(f"/jobs/{job['id']}/cancel", headers=headers)
        assert r.status_code == 409


# ── POST /jobs/{id}/requeue ───────────────────────────────────────────────────

class TestRequeueJob:
    def test_requeues_dead_job(self, client, headers, redis_client):
        job = client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers).json()
        client.post(f"/jobs/{job['id']}/cancel", headers=headers)
        redis_client.flushdb()
        r = client.post(f"/jobs/{job['id']}/requeue", headers=headers)
        assert r.status_code == 200
        assert r.json()["status"] == "pending"
        assert redis_client.llen("queue:normal") == 1

    def test_cannot_requeue_pending_job(self, client, headers):
        job = client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers).json()
        r = client.post(f"/jobs/{job['id']}/requeue", headers=headers)
        assert r.status_code == 409


# ── GET /workers ──────────────────────────────────────────────────────────────

class TestWorkers:
    def test_empty_when_no_workers(self, client, headers):
        r = client.get("/workers", headers=headers)
        assert r.status_code == 200
        assert r.json() == []

    def test_shows_registered_worker(self, client, headers, redis_client):
        redis_client.set("worker:heartbeat:test-worker-1", __import__("time").time())
        r = client.get("/workers", headers=headers)
        assert any(w["worker_id"] == "test-worker-1" for w in r.json())


# ── GET /metrics ──────────────────────────────────────────────────────────────

class TestMetrics:
    def test_returns_queue_depths(self, client, headers):
        r = client.get("/metrics", headers=headers)
        assert r.status_code == 200
        data = r.json()
        assert "queue_depths" in data
        assert "high" in data["queue_depths"]
        assert "delayed_count" in data
        assert "jobs_by_status" in data

    def test_depth_increments_on_enqueue(self, client, headers):
        client.post("/jobs", json={"type": "test.echo", "payload": {}}, headers=headers)
        r = client.get("/metrics", headers=headers)
        assert r.json()["queue_depths"]["normal"] >= 1
