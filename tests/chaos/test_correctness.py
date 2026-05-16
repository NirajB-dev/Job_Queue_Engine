"""
End-to-end correctness tests against the full Docker Compose stack.

Covers: idempotency, DLQ promotion, retry count, requeue, priority ordering,
and graceful cancel.

Run: pytest tests/chaos/test_correctness.py -v
"""
from __future__ import annotations

import time
import uuid

from tests.chaos.conftest import BASE_URL, wait_for_job_status


class TestIdempotency:
    def test_duplicate_key_returns_409(self, api):
        key = f"idem-{uuid.uuid4()}"
        r1 = api.post(f"{BASE_URL}/jobs", json={"type": "example.echo", "payload": {}, "idempotency_key": key})
        r2 = api.post(f"{BASE_URL}/jobs", json={"type": "example.echo", "payload": {}, "idempotency_key": key})
        assert r1.status_code == 201
        assert r2.status_code == 409

    def test_unique_keys_both_created(self, api):
        r1 = api.post(f"{BASE_URL}/jobs", json={"type": "example.echo", "payload": {}, "idempotency_key": f"k-{uuid.uuid4()}"})
        r2 = api.post(f"{BASE_URL}/jobs", json={"type": "example.echo", "payload": {}, "idempotency_key": f"k-{uuid.uuid4()}"})
        assert r1.status_code == 201
        assert r2.status_code == 201
        assert r1.json()["id"] != r2.json()["id"]


class TestDLQPromotion:
    def test_job_reaches_dead_after_max_attempts(self, api):
        """A job that always fails should exhaust retries and become dead."""
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.fail",
            "payload": {"message": "chaos: always fail"},
            "max_attempts": 1,
            "priority": 0,  # HIGH — processed quickly
        })
        assert r.status_code == 201
        job_id = r.json()["id"]

        job = wait_for_job_status(api, job_id, "dead", timeout=30)
        assert job["status"] == "dead"
        assert job["attempts"] == 1
        assert "chaos: always fail" in job["error"]

    def test_attempts_exhausted_match_max_attempts(self, api):
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.fail",
            "payload": {},
            "max_attempts": 2,
            "priority": 0,
        })
        job_id = r.json()["id"]
        job = wait_for_job_status(api, job_id, "dead", timeout=60)
        assert job["attempts"] == 2

    def test_attempts_endpoint_records_failure(self, api):
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.fail",
            "payload": {},
            "max_attempts": 1,
            "priority": 0,
        })
        job_id = r.json()["id"]
        wait_for_job_status(api, job_id, "dead", timeout=30)

        attempts = api.get(f"{BASE_URL}/jobs/{job_id}/attempts").json()
        assert len(attempts) == 1
        assert attempts[0]["attempt"] == 1


class TestRequeue:
    def test_dead_job_requeued_and_completed(self, api):
        """Requeue a dead job that can now succeed (change type is not possible,
        so we test requeue mechanics: status reset and re-enqueue.)"""
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.fail",
            "payload": {},
            "max_attempts": 1,
            "priority": 0,
        })
        job_id = r.json()["id"]
        wait_for_job_status(api, job_id, "dead", timeout=30)

        # Requeue puts it back to pending — it will fail again, but the mechanics work
        rq = api.post(f"{BASE_URL}/jobs/{job_id}/requeue")
        assert rq.status_code == 200
        assert rq.json()["status"] == "pending"
        assert rq.json()["attempts"] == 0

    def test_cannot_requeue_pending_job(self, api):
        r = api.post(f"{BASE_URL}/jobs", json={"type": "example.echo", "payload": {}})
        job_id = r.json()["id"]
        rq = api.post(f"{BASE_URL}/jobs/{job_id}/requeue")
        assert rq.status_code == 409


class TestCancel:
    def test_cancel_pending_job(self, api):
        """A pending job (not yet picked up) can be cancelled via the API."""
        # Use a delayed job so the worker won't grab it before we cancel
        future = time.time() + 3600
        from datetime import datetime, timezone
        run_at = datetime.fromtimestamp(future, tz=timezone.utc).isoformat()
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.echo",
            "payload": {},
            "run_at": run_at,
        })
        assert r.status_code == 201
        job_id = r.json()["id"]

        cancel = api.post(f"{BASE_URL}/jobs/{job_id}/cancel")
        assert cancel.status_code == 200
        assert cancel.json()["status"] == "dead"

    def test_cancel_already_dead_job_returns_409(self, api):
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.fail",
            "payload": {},
            "max_attempts": 1,
            "priority": 0,
        })
        job_id = r.json()["id"]
        wait_for_job_status(api, job_id, "dead", timeout=30)
        cancel = api.post(f"{BASE_URL}/jobs/{job_id}/cancel")
        assert cancel.status_code == 409


class TestPriorityOrdering:
    def test_high_priority_job_completes_before_low(self, api):
        """
        Enqueue a LOW then a HIGH job and verify the HIGH job finishes first.
        Uses example.sleep(seconds=2) on LOW so the HIGH job—which does a fast
        echo—has time to finish while the LOW job is still running.
        """
        low = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.sleep",
            "payload": {"seconds": 5},
            "priority": 2,  # LOW
        })
        high = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.echo",
            "payload": {"x": 1},
            "priority": 0,  # HIGH
        })
        assert low.status_code == 201
        assert high.status_code == 201

        high_job = wait_for_job_status(api, high.json()["id"], "done", timeout=20)
        low_job = api.get(f"{BASE_URL}/jobs/{low.json()['id']}").json()

        # High finished first: it should be done, LOW still running or done later
        assert high_job["status"] == "done"
        assert high_job["completed_at"] is not None

        # If LOW finished too, its completed_at should be >= HIGH's
        if low_job["status"] == "done":
            assert low_job["completed_at"] >= high_job["completed_at"]


class TestDelayedJobPromotion:
    def test_future_job_runs_after_delay(self, api):
        """A job with run_at in 5s should not be pending immediately but runs after."""
        from datetime import datetime, timedelta, timezone
        run_at = (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat()

        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.echo",
            "payload": {"delayed": True},
            "run_at": run_at,
        })
        assert r.status_code == 201
        job_id = r.json()["id"]

        # Should still be pending immediately after creation
        immediate = api.get(f"{BASE_URL}/jobs/{job_id}").json()
        assert immediate["status"] == "pending"

        # After 10s it should be done (scheduler promoted + worker processed)
        job = wait_for_job_status(api, job_id, "done", timeout=20)
        assert job["status"] == "done"
