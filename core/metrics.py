from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ── API / queue-side gauges (set on each Prometheus scrape) ───────────────────

QUEUE_DEPTH = Gauge(
    "job_queue_depth",
    "Current number of jobs waiting in a priority queue",
    ["priority"],
)

DELAYED_JOBS = Gauge(
    "job_queue_delayed",
    "Current number of jobs sitting in the delayed ZSET",
)

JOBS_BY_STATUS = Gauge(
    "job_queue_jobs_by_status",
    "Current job count broken down by status (sourced from Postgres)",
    ["status"],
)

# ── Worker-side counters and histograms (incremented per worker process) ──────

JOBS_PROCESSED = Counter(
    "job_worker_jobs_processed_total",
    "Total jobs completed successfully",
    ["job_type"],
)

JOBS_FAILED = Counter(
    "job_worker_jobs_failed_total",
    "Total jobs moved to the dead-letter queue",
    ["job_type", "reason"],  # reason: max_attempts | no_handler
)

JOB_DURATION = Histogram(
    "job_worker_job_duration_seconds",
    "End-to-end handler execution time",
    ["job_type"],
    buckets=[0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300],
)
