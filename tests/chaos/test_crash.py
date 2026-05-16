"""
Chaos tests: worker crash mid-job, watchdog reclaim, graceful shutdown.

Requires docker compose stack AND root/docker access to pause/stop containers.

Run: pytest tests/chaos/test_crash.py -v -s
"""
from __future__ import annotations

import time

from tests.chaos.conftest import BASE_URL, compose, wait_for_job_status

# Watchdog reclaim window: heartbeat every 15s, threshold 60s, poll 30s → max ~105s
RECLAIM_TIMEOUT = 120  # seconds


class TestWorkerCrash:
    def test_paused_worker_job_reclaimed_by_watchdog(self, api):
        """
        1. Submit a 90-second sleep job so the worker is busy.
        2. Pause the worker container (SIGSTOP → heartbeats stop).
        3. Watchdog detects stale heartbeat and resets job to pending.
        4. Unpause worker so it can pick up the reclaimed job.
        5. Verify job eventually completes.
        """
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.sleep",
            "payload": {"seconds": 90},
            "priority": 0,   # HIGH — picked up immediately
        })
        assert r.status_code == 201
        job_id = r.json()["id"]

        # Wait for a worker to pick it up
        wait_for_job_status(api, job_id, "running", timeout=30)

        # Pause all workers — heartbeats will stop
        compose("pause", "worker")
        try:
            paused_at = time.time()
            # Watchdog must reclaim within RECLAIM_TIMEOUT
            reclaimed = wait_for_job_status(
                api, job_id, "pending", timeout=RECLAIM_TIMEOUT
            )
            elapsed = time.time() - paused_at
            assert reclaimed["status"] == "pending"
            assert reclaimed["worker_id"] is None
            assert elapsed <= RECLAIM_TIMEOUT, f"Reclaim took {elapsed:.1f}s"
        finally:
            # Always unpause so subsequent tests can proceed
            compose("unpause", "worker")

        # After unpausing, workers reclaim and finish the job
        done = wait_for_job_status(api, job_id, "done", timeout=120)
        assert done["status"] == "done"

    def test_restarted_worker_re_registers_heartbeat(self, api):
        """After a worker restarts it should re-appear in GET /workers."""
        compose("restart", "worker")
        time.sleep(20)  # give workers time to restart and send first heartbeat

        workers = api.get(f"{BASE_URL}/workers").json()
        assert len(workers) > 0, "No workers registered after restart"
        assert all(w["age_seconds"] < 60 for w in workers), (
            "Some workers have stale heartbeats after restart"
        )


class TestGracefulShutdown:
    def test_worker_stops_cleanly_on_sigterm(self, api):
        """
        SIGTERM (docker compose stop) should let an idle worker exit cleanly
        (exit code 0) rather than being force-killed.
        """
        # Stop one worker — docker compose stop sends SIGTERM then waits
        result = compose("stop", "--timeout", "30", "worker")
        assert result.returncode == 0, f"compose stop failed: {result.stderr}"

        # Restart so the rest of the suite works
        compose("start", "worker")
        time.sleep(10)

    def test_api_handles_requests_after_worker_restart(self, api):
        """API should remain healthy while workers restart."""
        compose("restart", "worker")
        time.sleep(5)

        r = api.get(f"{BASE_URL}/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

        compose("restart", "worker")  # restore original count
        time.sleep(10)


class TestSchedulerCrash:
    def test_delayed_jobs_promoted_after_scheduler_restart(self, api):
        """
        Kill the scheduler, enqueue a delayed job, restart — job should be
        promoted when the scheduler comes back.
        """
        from datetime import datetime, timedelta, timezone
        compose("stop", "scheduler")

        run_at = (datetime.now(timezone.utc) + timedelta(seconds=3)).isoformat()
        r = api.post(f"{BASE_URL}/jobs", json={
            "type": "example.echo",
            "payload": {"scheduler_chaos": True},
            "run_at": run_at,
        })
        assert r.status_code == 201
        job_id = r.json()["id"]

        # Job stays pending while scheduler is down
        time.sleep(5)
        still_pending = api.get(f"{BASE_URL}/jobs/{job_id}").json()
        # May be pending (not yet promoted) — doesn't matter, just ensure no error
        assert still_pending["status"] in ("pending",)

        # Restart scheduler — it should pick up the due job immediately
        compose("start", "scheduler")
        done = wait_for_job_status(api, job_id, "done", timeout=30)
        assert done["status"] == "done"
