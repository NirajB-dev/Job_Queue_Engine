# Job Queue Engine

A production-grade distributed job queue and task processing engine built from scratch — demonstrating async systems design, failure handling, priority queuing, at-least-once delivery, retry with exponential backoff, dead-letter queues, observability, and chaos engineering.

**Stack:** Python · FastAPI · Redis · PostgreSQL · Docker · GKE Autopilot · Cloud Run · Prometheus · Grafana · k6

---

## Quick Start (Local)

```bash
git clone https://github.com/NirajB-dev/Job_Queue_Engine
cd Job_Queue_Engine
cp .env.example .env
docker compose up
```

The full stack starts in dependency order: Postgres → Redis → migrations → API + 2 workers + scheduler + watchdog.

```bash
# Submit a job
curl -X POST http://localhost:8080/jobs \
  -H "X-API-Key: local-dev-key" \
  -H "Content-Type: application/json" \
  -d '{"type": "example.echo", "payload": {"msg": "hello"}}'

# Check its status
curl http://localhost:8080/jobs/<id> -H "X-API-Key: local-dev-key"

# View queue metrics
curl http://localhost:8080/metrics -H "X-API-Key: local-dev-key"
```

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Cloud Run (FastAPI)                   │
│  POST /jobs  GET /jobs  GET /workers  GET /metrics   │
│  GET /prometheus  (Prometheus scrape endpoint)       │
└────────────┬──────────────────────┬─────────────────┘
             │                      │
             ▼                      ▼
  ┌──────────────────┐   ┌──────────────────────────┐
  │ Memorystore Redis│   │  Cloud SQL PostgreSQL 15  │
  │                  │   │                           │
  │  queue:high      │   │  jobs          (source    │
  │  queue:normal    │   │  job_attempts   of truth) │
  │  queue:low       │   │                           │
  │  delayed:jobs    │   └──────────────────────────┘
  │  worker:hb:*     │              ▲
  └──────┬───────────┘              │
         │  BLPOP                   │ write result/error
         ▼                          │
┌─────────────────────────────────────────────────────┐
│                 GKE Autopilot                        │
│                                                      │
│  worker ×N  ──────── BLPOP queue:high/normal/low    │
│  scheduler  ──────── ZRANGEBYSCORE delayed → lists  │
│  watchdog   ──────── heartbeat scan → reclaim       │
└─────────────────────────────────────────────────────┘
```

### Job Lifecycle

```
1. Client  →  POST /jobs              →  Postgres (status=pending)
                                      →  Redis enqueue (or delayed ZSET)
2. Worker  ←  BLPOP queue:high …      ←  Redis
3. Worker  →  UPDATE status=running   →  Postgres
4. Worker  →  execute handler
   ├─ success  →  UPDATE status=done, result=…    →  Postgres
   └─ failure
       ├─ attempts < max  →  reschedule with backoff  →  delayed ZSET
       └─ attempts ≥ max  →  INSERT job_attempts, status=dead  →  Postgres
5. Scheduler  →  ZRANGEBYSCORE 0..now  →  RPUSH to live queue  (every 1s)
6. Watchdog   →  scan stale heartbeats →  claim + re-enqueue   (every 30s)
```

---

## Priority Queue Design

Three Redis lists give strict priority with a single atomic BLPOP call:

```
BLPOP queue:high queue:normal queue:low 5
```

Redis returns the first available item from the highest-priority non-empty list. No polling, no starvation logic needed for HIGH and NORMAL. LOW jobs may starve if HIGH is always full — this trade-off is documented and acceptable for the use case.

| Priority | Redis key     | IntEnum value |
|----------|---------------|---------------|
| HIGH     | `queue:high`  | 0             |
| NORMAL   | `queue:normal`| 1             |
| LOW      | `queue:low`   | 2             |

---

## Reliability Guarantees

### At-Least-Once Delivery
Every worker refreshes a heartbeat key in Redis every 15 seconds. The watchdog scans all `worker:heartbeat:*` keys every 30 seconds and reclaims any running job whose worker heartbeat is >60s stale — resetting it to `pending` and re-enqueuing it. Verified by chaos test: SIGSTOP a worker, job reclaimed within 120s.

### Retry with Exponential Backoff + Jitter
```python
delay = min(10 × 2^attempt + U(0, 5), 3600)   # seconds
```
Jitter prevents retry storms when many jobs fail simultaneously. Failed jobs enter the `delayed:jobs` sorted set and are promoted back to the live queue by the scheduler when their `run_at` arrives.

### Dead-Letter Queue
After `max_attempts` failures the job is marked `dead` and a row is inserted into `job_attempts` with the full error trace and worker context. Dead jobs are visible via `GET /jobs?status=dead` and can be manually requeued via `POST /jobs/{id}/requeue`.

### Idempotency
Jobs submitted with an `idempotency_key` are deduplicated at the DB level via a `UNIQUE` constraint. A duplicate submission returns HTTP 409 rather than creating a new job.

### Graceful Shutdown
Workers trap SIGTERM and complete the current job before exiting. The `deregister_worker` call in the `finally` block removes the heartbeat key on clean shutdown so the watchdog does not reclaim jobs from a cleanly stopped worker.

---

## API Reference

All endpoints (except `GET /health` and `GET /prometheus`) require the `X-API-Key` header.

### Jobs

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Create and enqueue a job |
| `GET` | `/jobs` | List jobs (filter by `status`, `type`, `priority`; paginate with `limit`/`offset`) |
| `GET` | `/jobs/{id}` | Get a single job by UUID |
| `GET` | `/jobs/{id}/attempts` | Get DLQ attempt history for a job |
| `POST` | `/jobs/{id}/cancel` | Cancel a pending job (sets status=dead) |
| `POST` | `/jobs/{id}/requeue` | Reset a dead job to pending and re-enqueue it |

#### Create Job Request Body

```json
{
  "type": "example.echo",
  "payload": { "any": "json" },
  "priority": 1,
  "max_attempts": 3,
  "idempotency_key": "optional-string",
  "run_at": "2026-01-01T12:00:00Z"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | required | Handler name (e.g. `example.echo`) |
| `payload` | object | required | Arbitrary JSON passed to the handler |
| `priority` | int | `1` | 0=HIGH, 1=NORMAL, 2=LOW |
| `max_attempts` | int | `3` | Max retries before DLQ (1–10) |
| `idempotency_key` | string | null | Deduplication key |
| `run_at` | datetime | now | Schedule for future execution |

### Workers

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/workers` | List all workers with live heartbeats in Redis |

### Metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metrics` | JSON: queue depths, delayed count, jobs by status |
| `GET` | `/prometheus` | Prometheus text format (no auth required — for scraping) |

---

## Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `job_queue_depth` | Gauge | `priority` | Jobs waiting in each live queue |
| `job_queue_delayed` | Gauge | — | Jobs in delayed ZSET |
| `job_queue_jobs_by_status` | Gauge | `status` | Job count by status (from Postgres) |
| `job_worker_jobs_processed_total` | Counter | `job_type` | Successful completions per worker process |
| `job_worker_jobs_failed_total` | Counter | `job_type`, `reason` | DLQ entries (`max_attempts` or `no_handler`) |
| `job_worker_job_duration_seconds` | Histogram | `job_type` | Handler execution time (10 buckets, 50ms–600s) |

Import `grafana/dashboard.json` into Grafana (datasource: Prometheus) for pre-built panels covering queue depth, delayed jobs, DLQ depth stat, jobs by status, throughput rate, and duration p50/p95/p99.

---

## Running Locally

### Docker Compose (recommended)

```bash
cp .env.example .env   # edit POSTGRES_PASSWORD and API_KEY if desired
docker compose up      # postgres → redis → migrate → api + workers + scheduler + watchdog
```

- API available at `http://localhost:8080`
- Postgres at `localhost:5432` (user: jobqueue_user, db: jobqueue)
- Redis at `localhost:6379`

To run with 4 workers:

```bash
docker compose up --scale worker=4
```

### Manual (requires local Postgres + Redis)

```bash
# Start dependencies
docker run -d -p 5432:5432 -e POSTGRES_DB=jobqueue_test \
  -e POSTGRES_USER=jobqueue_user -e POSTGRES_PASSWORD=testpassword postgres:15
docker run -d -p 6379:6379 redis:7

# Install dependencies
pip install -r requirements.txt

# Run migrations
DATABASE_URL=postgresql://jobqueue_user:testpassword@localhost/jobqueue_test \
  alembic upgrade head

# Start API
DATABASE_URL=... REDIS_URL=redis://localhost:6379 API_KEY=dev \
  uvicorn api.main:app --host 0.0.0.0 --port 8080

# Start worker
DATABASE_URL=... REDIS_URL=... API_KEY=dev python -m worker.worker

# Start scheduler
REDIS_URL=... DATABASE_URL=... API_KEY=dev python -m scheduler.scheduler

# Start watchdog
DATABASE_URL=... REDIS_URL=... API_KEY=dev python -m watchdog.watchdog
```

---

## Running Tests

### Unit + Integration Tests (CI)

```bash
# With containers running from manual setup above
DATABASE_URL=postgresql://jobqueue_user:testpassword@localhost/jobqueue_test \
REDIS_URL=redis://localhost:6379 \
API_KEY=test-api-key \
ENVIRONMENT=test \
  pytest tests/ -v --ignore=tests/chaos
```

**Test counts:** 90 tests across 7 modules

| Module | Tests | What it covers |
|--------|-------|----------------|
| `test_repository.py` | 14 | All DB access patterns, idempotency, claim_stale_jobs |
| `test_queue.py` | 16 | Enqueue routing, BLPOP priority order, promote_delayed, heartbeats |
| `test_worker.py` | 9 | Success, retry reschedule, DLQ at max_attempts, unknown type |
| `test_scheduler.py` | 6 | Delayed job promotion, partial, idempotent |
| `test_watchdog.py` | 7 | Fresh vs stale worker, reclaim, heartbeat removal |
| `test_api.py` | 24 | Auth, CRUD, filtering, pagination, cancel, requeue |
| `test_prometheus.py` | 9 | Scrape endpoint, metric names, gauge updates |
| `test_registry.py` | 4 | Handler registration |
| `test_retry.py` | 4 | Backoff formula, jitter bounds |

### Chaos + Correctness Tests

Requires `docker compose up` to be running.

```bash
# Correctness (~36s) — idempotency, DLQ, retry, requeue, cancel, priority, delayed
pytest tests/chaos/test_correctness.py -v

# Crash/chaos (~3min) — worker pause→reclaim, restart→heartbeat, graceful shutdown
pytest tests/chaos/test_crash.py -v -s

# All chaos tests
pytest tests/chaos/ -v
```

---

## Load Testing (k6)

Install k6: https://k6.io/docs/get-started/installation/

```bash
# Quick smoke test (1 VU, 30 iters, ~10s)
k6 run k6/smoke_test.js

# Full load test (ramp to 100 VUs, 3 min total)
k6 run k6/load_test.js

# Against Cloud Run
BASE_URL=https://job-queue-api-xxx.run.app API_KEY=<key> \
  k6 run k6/load_test.js
```

**Load test profile:**
- Stage 1: 0 → 100 VUs over 30s (ramp up)
- Stage 2: 100 VUs for 2 minutes (sustained)
- Stage 3: 100 → 0 over 30s (ramp down)
- Traffic mix: 85% POST /jobs · 10% GET /jobs · 5% GET /prometheus
- **Thresholds:** p99 enqueue latency < 500ms, error rate < 1%

---

## Database Schema

```sql
CREATE TABLE jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type             VARCHAR(100) NOT NULL,
    payload          JSON NOT NULL,
    priority         SMALLINT DEFAULT 1,       -- 0=HIGH 1=NORMAL 2=LOW
    status           VARCHAR(20) DEFAULT 'pending',
    attempts         INT DEFAULT 0,
    max_attempts     INT DEFAULT 3,
    idempotency_key  VARCHAR(255) UNIQUE,
    scheduled_at     TIMESTAMPTZ DEFAULT now(),
    run_at           TIMESTAMPTZ DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    result           JSON,
    error            TEXT,
    worker_id        VARCHAR(100)
);

CREATE TABLE job_attempts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    failed_at   TIMESTAMPTZ NOT NULL,
    error       TEXT NOT NULL,
    worker_id   VARCHAR(100) NOT NULL
);
```

Indexes: `(status, run_at)` for worker polling · `(worker_id, status)` for watchdog reclaim · `(job_id)` on attempts.

---

## Project Structure

```
Job_Queue_Engine/
├── api/
│   ├── main.py               # FastAPI app with lifespan (DB + Redis connections)
│   ├── dependencies.py       # get_repo / get_queue FastAPI depends
│   ├── middleware/
│   │   └── auth.py           # X-API-Key header enforcement
│   └── routers/
│       ├── jobs.py           # POST/GET /jobs, cancel, requeue, attempts
│       ├── workers.py        # GET /workers (live heartbeat scan)
│       ├── metrics.py        # GET /metrics (JSON)
│       └── prometheus_metrics.py  # GET /prometheus (text/plain scrape)
├── core/
│   ├── config.py             # pydantic-settings (env vars + Secret Manager)
│   ├── models.py             # Job, JobAttempt, Priority, JobStatus, CreateJobRequest
│   ├── repository.py         # JobRepository — all Postgres access
│   ├── queue.py              # RedisQueue — enqueue, BLPOP, heartbeats, promote
│   ├── registry.py           # HandlerRegistry — maps job type → callable
│   └── metrics.py            # prometheus_client metric definitions
├── worker/
│   ├── worker.py             # BLPOP loop, handler dispatch, retry/DLQ, metrics
│   ├── retry.py              # backoff_seconds(attempt) with jitter
│   ├── handlers/
│   │   ├── __init__.py       # side-effect import — registers all handlers
│   │   └── example.py        # example.echo, example.sleep, example.fail
│   └── Dockerfile
├── scheduler/
│   └── scheduler.py          # Promotes delayed ZSET → live queues every 1s
├── watchdog/
│   └── watchdog.py           # Heartbeat monitor + job reclaim every 30s
├── migrations/
│   ├── env.py                # DATABASE_URL injected from env at runtime
│   └── versions/
│       └── 0001_create_jobs_tables.py
├── k8s/
│   ├── serviceaccount.yaml   # Workload Identity binding
│   ├── worker.yaml           # Deployment (2 replicas)
│   ├── scheduler.yaml        # Deployment (1 replica)
│   └── watchdog.yaml         # Deployment (1 replica)
├── tests/
│   ├── test_repository.py    # 14 tests
│   ├── test_queue.py         # 16 tests
│   ├── test_worker.py        # 9 tests
│   ├── test_scheduler.py     # 6 tests
│   ├── test_watchdog.py      # 7 tests
│   ├── test_api.py           # 24 tests
│   ├── test_prometheus.py    # 9 tests
│   ├── test_registry.py      # 4 tests
│   ├── test_retry.py         # 4 tests
│   └── chaos/
│       ├── conftest.py       # api fixture, compose helper, wait_for_status
│       ├── test_correctness.py  # 11 end-to-end correctness tests
│       └── test_crash.py     # 5 chaos tests (pause/stop/restart containers)
├── grafana/
│   └── dashboard.json        # Importable Grafana dashboard (6 panels)
├── k6/
│   ├── load_test.js          # Ramp to 100 VUs, 3-min sustained load
│   └── smoke_test.js         # 1 VU, 30 iters, ~10s
├── docker-compose.yml        # Full local stack (7 services)
├── .env.example
├── .github/workflows/
│   ├── ci.yml                # pytest (90 tests) + ruff on every PR
│   └── deploy.yml            # build → migrate → Cloud Run + GKE on merge to main
├── requirements.txt
└── alembic.ini
```

---

## GCP Infrastructure

| Resource | Name |
|----------|------|
| Project | `job-queue-engine` |
| Region | `us-central1` |
| GKE Cluster | `job-queue-cluster` (Autopilot) |
| Cloud SQL | `job-queue-postgres` (PostgreSQL 15, db-f1-micro) |
| Memorystore | `job-queue-redis` (Redis 7, 1 GB) |
| Artifact Registry | `us-central1-docker.pkg.dev/job-queue-engine/job-queue-images` |
| K8s Namespace | `job-queue` |
| Service Account | `job-queue-sa@job-queue-engine.iam.gserviceaccount.com` |
| WIF Pool | `github-actions-pool` / `github-provider` |

Secrets in Secret Manager: `database-url`, `redis-host`, `api-key`, `db-password`.

---

## CI/CD Pipeline

```
PR opened
  └── ci.yml
        ├── pytest tests/ --ignore=tests/chaos   (90 tests)
        └── ruff check .

Merge to main
  └── deploy.yml
        ├── Build API + Worker images → Artifact Registry
        ├── Create/update migrate Cloud Run Job → run alembic upgrade head
        ├── Deploy API → Cloud Run
        │     (secrets: DATABASE_URL, API_KEY, REDIS_HOST from Secret Manager)
        │     (service-account: job-queue-sa — has secretAccessor role)
        └── Apply k8s/ manifests → GKE (worker + scheduler + watchdog)
```

Auth uses Workload Identity Federation — no long-lived service account keys stored in GitHub.

---

## Build Progress

| Step | Description | Status |
|------|-------------|--------|
| Infra | GCP resources, GKE, WIF | ✅ |
| CI/CD | GitHub Actions (pytest + ruff + deploy) | ✅ |
| 1 | PostgreSQL schema + Alembic migrations | ✅ |
| 2 | Job model + JobRepository | ✅ |
| 3 | RedisQueue abstraction | ✅ |
| 4 | Worker loop + HandlerRegistry + retry logic | ✅ |
| 5 | Scheduler (delayed job promotion) | ✅ |
| 6 | Watchdog (heartbeat monitor + reclaim) | ✅ |
| 7 | FastAPI control plane (full API) | ✅ |
| 8 | Prometheus metrics + Grafana dashboard | ✅ |
| 9 | Docker Compose (local full-stack) | ✅ |
| 10 | k6 load test | ✅ |
| 11 | Chaos + correctness tests | ✅ |
