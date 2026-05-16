"""
Shared fixtures for chaos and correctness tests.

Requires the full Docker Compose stack to be running:
    docker compose up -d

Run:
    pytest tests/chaos/ -v --tb=short
"""
from __future__ import annotations

import subprocess
import time

import pytest
import requests


BASE_URL = "http://localhost:8080"
API_KEY = "local-dev-key"


# ── HTTP client ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def api() -> requests.Session:
    s = requests.Session()
    s.headers.update({"X-API-Key": API_KEY, "Content-Type": "application/json"})
    s.base_url = BASE_URL  # type: ignore[attr-defined]
    return s


# ── Docker Compose helpers ────────────────────────────────────────────────────

def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a docker compose sub-command from the project root."""
    return subprocess.run(
        ["docker", "compose", *args],
        check=check,
        capture_output=True,
        text=True,
    )


# ── Polling helper ────────────────────────────────────────────────────────────

def wait_for_job_status(
    api: requests.Session,
    job_id: str,
    target_status: str | set[str],
    timeout: float = 30.0,
    interval: float = 1.0,
) -> dict:
    """Poll GET /jobs/{id} until status matches; raises TimeoutError on expiry."""
    if isinstance(target_status, str):
        target_status = {target_status}
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = api.get(f"{BASE_URL}/jobs/{job_id}")
        r.raise_for_status()
        job = r.json()
        if job["status"] in target_status:
            return job
        time.sleep(interval)
    raise TimeoutError(
        f"Job {job_id} did not reach {target_status} within {timeout}s "
        f"(last status: {job['status']})"
    )
