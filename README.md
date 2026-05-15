# Job Queue Engine

A production-grade distributed job queue and task processing engine built from scratch — demonstrating async systems, failure handling, priority queuing, at-least-once delivery, retry with exponential backoff, dead-letter queues, and observability.

**Stack:** Python · FastAPI · Redis · PostgreSQL · Docker · GKE · Cloud Run · Prometheus · k6

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Cloud Run (FastAPI)                   │
│  POST /jobs  GET /jobs  GET /workers  GET /metrics   │
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
│  worker ×N  ──────── BLPOP queue:high/normal/low ─► │
│  scheduler  ──────── ZRANGEBYSCORE delayed → lists   │
│  watchdog   ──────── heartbeat scan → reclaim        │
└─────────────────────────────────────────────────────┘
```

### How a job flows through the system

```
1. Client  →  POST /jobs              →  Postgres (status=pending)
                                      →  Redis enqueue (or delayed ZSET)
2. Worker  ←  BLPOP queue:high …      ←  Redis
3. Worker  →  UPDATE status=running   →  Postgres
4. Worker  →  execute handler
   ├─ success  →  UPDATE status=done, result=…  →  Postgres
   └─ failure
       ├─ attempts < max  →  reschedule with backoff  →  delayed ZSET
       └─ attempts >= max →  INSERT job_attempts, status=dead  →  Postgres (DLQ)
5. Scheduler  →  ZRANGEBYSCORE 0..now  →  RPUSH to live queue  (every 1s)
6. Watchdog   →  scan stale heartbeats →  claim + re-enqueue    (every 30s)
```

---

## Priority Queue Design

Three Redis lists give strict priority with a single atomic BLPOP call:

```
BLPOP queue:high queue:normal queue:low 5
```

Redis returns the first available item from the highest-priority non-empty list. No polling, no starvation logic needed for HIGH and NORMAL. LOW jobs may starve if HIGH is always full — this trade-off is documented and acceptable for the use case.

| Priority | Redis key | IntEnum value |
|---|---|---|
| HIGH | `queue:high` | 0 |
| NORMAL | `queue:normal` | 1 |
| LOW | `queue:low` | 2 |

---

## Reliability Guarantees

### At-least-once delivery
Every worker refreshes a heartbeat key in Redis every 15 seconds. The watchdog scans all `worker:heartbeat:*` keys every 30 seconds and reclaims any running job whose worker heartbeat is >60s stale — resetting it to `pending` and re-enqueuing it.

### Retry with exponential backoff + jitter
```python
delay = min(10 * 2^attempt + U(0, 5), 3600)   # seconds
```
Jitter prevents retry storms when many jobs fail simultaneously. Failed jobs go into the `delayed:jobs` sorted set and are promoted back to the live queue by the scheduler when their `run_at` arrives.

### Dead-letter queue
After `max_attempts` failures the job is marked `dead` and a row is inserted into `job_attempts` with the full error and worker context. Dead jobs are visible via `GET /jobs?status=dead` and can be manually requeued via `POST /jobs/{id}/requeue`.

### Idempotency
Jobs submitted with an `idempotency_key` are deduplicated at the DB level via a `UNIQUE` constraint. A duplicate submission returns the existing job rather than creating a new one.

---

## Project Structure

```
Job_Queue_Engine/
├── api/
│   ├── main.py              # FastAPI app (health check; full router in step 7)
│   ├── routers/             # jobs, workers, metrics endpoints
│   ├── middleware/          # API key auth, request logging
│   └── Dockerfile
├── core/
│   ├── config.py            # Settings from env vars / Secret Manager
│   ├── models.py            # Job, JobAttempt Pydantic models + Priority enum
│   ├── repository.py        # JobRepository — all Postgres access
│   ├── queue.py             # RedisQueue — enqueue, BLPOP, heartbeats, metrics
│   └── registry.py          # HandlerRegistry — maps job type → callable
├── worker/
│   ├── worker.py            # BLPOP loop, handler dispatch, retry/DLQ logic
│   ├── retry.py             # backoff_seconds(attempt) with jitter
│   ├── handlers/
│   │   ├── __init__.py      # imports all handlers (side-effect registration)
│   │   └── example.py       # example.echo, example.sleep, example.fail
│   └── Dockerfile
├── scheduler/
│   └── scheduler.py         # Promotes delayed ZSET → live queues every 1s
├── watchdog/
│   └── watchdog.py          # Heartbeat monitor + job reclaim every 30s
├── migrations/
│   ├── env.py               # DATABASE_URL injected from env at runtime
│   └── versions/
│       └── 0001_create_jobs_tables.py
├── k8s/
│   ├── serviceaccount.yaml  # Workload Identity binding
│   ├── worker.yaml          # Deployment (2 replicas)
│   ├── scheduler.yaml       # Deployment (1 replica)
│   └── watchdog.yaml        # Deployment (1 replica)
├── tests/                   # pytest integration test suite
│   ├── test_repository.py   # 14 tests — all DB access patterns
│   ├── test_queue.py        # 16 tests — Redis queue operations
│   ├── test_worker.py       # 9 tests  — worker dispatch, retry, DLQ
│   ├── test_scheduler.py    # 6 tests  — delayed job promotion
│   ├── test_watchdog.py     # 7 tests  — stale worker reclaim
│   ├── test_registry.py     # 4 tests  — handler registration
│   └── test_retry.py        # 4 tests  — backoff values
├── .github/workflows/
│   ├── ci.yml               # pytest + ruff on every PR
│   └── deploy.yml           # build → migrate → Cloud Run + GKE on merge to main
├── requirements.txt
└── alembic.ini
```

---

## GCP Infrastructure

| Resource | Name |
|---|---|
| Project | `job-queue-engine` |
| Region | `us-central1` |
| GKE Cluster | `job-queue-cluster` (Autopilot) |
| Cloud SQL | `job-queue-postgres` (PostgreSQL 15, db-f1-micro) |
| Memorystore | `job-queue-redis` (Redis 7, 1 GB) |
| Artifact Registry | `us-central1-docker.pkg.dev/job-queue-engine/job-queue-images` |
| K8s Namespace | `job-queue` |
| Service Account | `job-queue-sa@job-queue-engine.iam.gserviceaccount.com` |
| WIF Pool | `github-actions-pool` / `github-provider` |

Secrets (stored in Secret Manager): `database-url`, `redis-host`, `db-password`, `api-key`.

---

## CI/CD Pipeline

```
PR opened
  └── ci.yml
        ├── pytest (against ephemeral Postgres + Redis containers)
        └── ruff lint

Merge to main
  └── deploy.yml
        ├── Build API + Worker images → Artifact Registry
        ├── Create/update migrate Cloud Run Job → run alembic upgrade head
        ├── Deploy API → Cloud Run
        └── Apply k8s/ manifests → GKE (worker + scheduler + watchdog)
```

Auth uses Workload Identity Federation — no long-lived service account keys stored in GitHub.

---

## Database Schema

```sql
-- jobs: source of truth for every job
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

-- job_attempts: DLQ history — one row per terminal failure
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

Indexes: `(status, run_at)` for worker polling, `(worker_id, status)` for watchdog reclaim, `(job_id)` on attempts.

---

## Key Components

### `core/repository.py` — JobRepository

| Method | Purpose |
|---|---|
| `create(req)` | Insert job, rollback on constraint violation |
| `get_by_id(id)` | Fetch by UUID, returns `None` if missing |
| `list(status, type, priority, limit, offset)` | Filtered + paginated |
| `update_status(id, status, ...)` | Sets status, timestamps, worker_id, result/error |
| `reschedule(id, run_at, error)` | Retry: reset to pending with future run_at |
| `move_to_dlq(id, attempt, ...)` | Mark dead + write job_attempts row atomically |
| `requeue(id)` | Reset dead → pending for manual retry |
| `claim_stale_jobs(worker_id)` | Watchdog: bulk reset running→pending |

### `core/queue.py` — RedisQueue

| Method | Purpose |
|---|---|
| `enqueue(job_id, priority, run_at)` | Immediate → RPUSH; future → ZADD delayed ZSET |
| `dequeue_blocking(timeout)` | `BLPOP queue:high queue:normal queue:low` |
| `promote_delayed()` | Move due jobs from ZSET to live lists (pipeline) |
| `register_heartbeat(worker_id)` | `SET worker:heartbeat:{id} {timestamp}` |
| `get_stale_workers(threshold_s)` | Scan heartbeat keys, return IDs with stale timestamps |
| `queue_depths()` | `LLEN` on all three lists (Prometheus) |

### `worker/worker.py` — Worker

Accepts `(conn, redis_client, registry)` — fully injectable, no global state.

```
run() → SIGTERM-aware BLPOP loop
  _maybe_heartbeat()   every 15s
  _process(job_id)
    update_status(RUNNING, attempts+1)
    dispatch handler
    → success:  update_status(DONE, result)
    → failure:  _on_failure(job, error)
        attempts < max → reschedule + enqueue to delayed ZSET
        attempts >= max → move_to_dlq
        unknown type → immediate DLQ
```

---

## Running Tests Locally

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

# Run tests
DATABASE_URL=postgresql://jobqueue_user:testpassword@localhost/jobqueue_test \
REDIS_URL=redis://localhost:6379 \
API_KEY=test-api-key \
ENVIRONMENT=test \
  pytest tests/ -v
```

---

## Build Progress

| Step | Description | Status |
|---|---|---|
| Infra | GCP resources, GKE, WIF | ✅ |
| CI/CD | GitHub Actions (pytest + ruff + deploy) | ✅ |
| 1 | PostgreSQL schema + Alembic migrations | ✅ |
| 2 | Job model + JobRepository | ✅ |
| 3 | RedisQueue abstraction | ✅ |
| 4 | Worker loop + HandlerRegistry + retry logic | ✅ |
| 5 | Scheduler (delayed job promotion) | ✅ |
| 6 | Watchdog (heartbeat monitor + reclaim) | ✅ |
| 7 | FastAPI control plane (full API) | ⬜ |
| 8 | Prometheus metrics + Grafana dashboard | ⬜ |
| 9 | Docker Compose (local full-stack) | ⬜ |
| 10 | k6 load test | ⬜ |
| 11 | Chaos + correctness tests | ⬜ |
