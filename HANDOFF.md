# Claude Code — Project Handoff & Context

Paste this entire file as your first message in a new Claude Code session to pick up exactly where we left off.

---

## What We're Building

A **Distributed Job Queue & Task Processing Engine** — a portfolio/interview project demonstrating backend engineering depth: async systems, failure handling, priority queuing, at-least-once delivery, retries with backoff, dead-letter queues, observability.

**Full spec is at:** `~/Job_Queue_Engine/Job_Queue_Engine_Spec.md`

---

## Stack

| Layer | Technology |
|---|---|
| API | FastAPI → deployed on Cloud Run |
| Workers | GKE Autopilot Deployment + HPA |
| Queue | Memorystore Redis 7 |
| Database | Cloud SQL PostgreSQL 15 |
| Observability | Google Managed Prometheus + Grafana |
| CI/CD | GitHub Actions + Workload Identity Federation |
| Secrets | Secret Manager |
| Images | Artifact Registry |

---

## GCP Infrastructure — Already Provisioned

| Resource | Name | Status |
|---|---|---|
| Project | `job-queue-engine` | ✅ |
| Region | `us-central1` | ✅ |
| GKE Cluster | `job-queue-cluster` (Autopilot) | ✅ Running |
| Cloud SQL | `job-queue-postgres` (PostgreSQL 15, db-f1-micro) | ✅ Runnable |
| Memorystore | `job-queue-redis` (Redis 7, 1GB) | ✅ Ready |
| Artifact Registry | `us-central1-docker.pkg.dev/job-queue-engine/job-queue-images` | ✅ |
| K8s Namespace | `job-queue` | ✅ |
| Service Account | `job-queue-sa@job-queue-engine.iam.gserviceaccount.com` | ✅ |
| WIF | `github-actions-pool` / `github-provider` | ✅ |

**All connection values are in Secret Manager. Retrieve with:**
```bash
cat ~/.env.gcp  # saved during bootstrap
# or
gcloud secrets versions access latest --secret=redis-host
gcloud secrets versions access latest --secret=db-connection-name
gcloud secrets versions access latest --secret=db-password
gcloud secrets versions access latest --secret=api-key
```

---

## Repository

- **GitHub:** https://github.com/NirajB-dev/Job_Queue_Engine.git
- **Local path (Cloud Shell):** `~/Job_Queue_Engine`
- **Main branch:** protected, CI/CD deploys on merge

**Current commits on main:**
```
47eb09e Merge pull request #1 from NirajB-dev/feat/cicd-github-actions
94aa7b7 feat: add GitHub Actions CI/CD pipeline
2bde716 chore: initial project scaffold
```

---

## CI/CD — Already Set Up

Two GitHub Actions workflows in `.github/workflows/`:

**`ci.yml`** — runs on every PR to main:
- pytest with ephemeral Postgres + Redis service containers
- ruff lint

**`deploy.yml`** — runs on every merge to main:
1. Build API + Worker Docker images → Artifact Registry
2. Run Alembic migrations via Cloud Run Job
3. Deploy API → Cloud Run
4. Roll out worker/scheduler/watchdog → GKE

Auth via Workload Identity Federation (no stored keys). GitHub secrets `WIF_PROVIDER` and `WIF_SERVICE_ACCOUNT` are already set.

---

## Project Folder Structure

```
~/Job_Queue_Engine/
├── api/
│   ├── main.py              # FastAPI app
│   ├── routers/
│   │   ├── jobs.py          # POST/GET /jobs, cancel, requeue
│   │   ├── workers.py       # GET /workers
│   │   └── metrics.py       # GET /metrics (Prometheus)
│   ├── middleware/          # API key auth, request logging
│   └── Dockerfile
├── core/
│   ├── config.py            # Settings from Secret Manager/env
│   ├── models.py            # Job + JobAttempt Pydantic models
│   ├── repository.py        # JobRepository (all DB access)
│   ├── queue.py             # RedisQueue abstraction
│   └── registry.py          # HandlerRegistry
├── worker/
│   ├── worker.py            # BLPOP loop + handler execution
│   ├── retry.py             # Exponential backoff logic
│   ├── handlers/            # Job type implementations
│   └── Dockerfile
├── scheduler/
│   └── scheduler.py         # Promotes delayed jobs ZSET → queue
├── watchdog/
│   └── watchdog.py          # Heartbeat monitor + job reclaim
├── migrations/              # Alembic migrations
├── k8s/                     # Kubernetes manifests
├── infra/                   # setup_wif.sh, terraform (future)
├── tests/                   # pytest suite
├── k6/                      # Load test scripts
├── grafana/                 # Dashboard JSON
├── .github/workflows/       # CI + CD workflows
├── requirements.txt         # (to be created)
└── .env.gcp                 # Local secrets reference (not committed)
```

---

## Workflow Rules — Follow These Every Time

1. **Always work on a feature branch** — never commit directly to main
   ```bash
   git checkout -b feat/step-N-description
   ```

2. **Always give expected output** for every command so it's easy to debug

3. **Remind to merge** when a step is tested and working:
   ```bash
   git push origin feat/step-N-description
   # open PR → merge → CD auto-deploys
   ```

4. **Branch naming convention:**
   - `feat/step-1-schema`
   - `feat/step-2-models`
   - `feat/step-3-redis-queue`
   - etc.

---

## Build Order — What's Done vs What's Next

| Step | Description | Status |
|---|---|---|
| Infra | GCP resources, K8s namespace, WIF | ✅ Done |
| CI/CD | GitHub Actions workflows | ✅ Done |
| **1** | **PostgreSQL schema + Alembic migrations** | ⬅ START HERE |
| 2 | Job model + JobRepository (core/models.py, core/repository.py) | ⬜ |
| 3 | RedisQueue abstraction (core/queue.py) | ⬜ |
| 4 | Worker loop + HandlerRegistry + retry logic | ⬜ |
| 5 | Scheduler thread | ⬜ |
| 6 | Watchdog thread | ⬜ |
| 7 | FastAPI control plane + Dockerfiles | ⬜ |
| 8 | Kubernetes manifests (worker/scheduler/watchdog) | ⬜ |
| 9 | Cloud Run deployment for API | ⬜ |
| 10 | Prometheus metrics + Grafana dashboard | ⬜ |
| 11 | k6 load test | ⬜ |
| 12 | Chaos + correctness tests (pytest) | ⬜ |

---

## Key Design Decisions Already Made

**Priority queuing:** Three Redis lists (`queue:high`, `queue:normal`, `queue:low`). Workers use `BLPOP queue:high queue:normal queue:low 5` — single atomic call, strict priority ordering.

**At-least-once delivery:** Worker updates a heartbeat key in Redis every 15s. Watchdog reclaims jobs from workers whose heartbeat is >60s stale.

**Retry backoff:** `delay = min(10 * 2^attempt + random(0, 5), 3600)`. Jitter prevents retry storms.

**Dead-letter queue:** After `max_attempts` failures → `status='dead'` + insert into `job_attempts`. Requeue via `POST /jobs/{id}/requeue`.

**PostgreSQL schema:**
```sql
-- jobs table
id UUID PRIMARY KEY DEFAULT gen_random_uuid()
type VARCHAR(100) NOT NULL
payload JSONB NOT NULL
priority SMALLINT DEFAULT 1  -- 0=HIGH 1=NORMAL 2=LOW
status VARCHAR(20) DEFAULT 'pending'  -- pending|running|done|failed|dead
attempts INT DEFAULT 0
max_attempts INT DEFAULT 3
idempotency_key VARCHAR(255) UNIQUE NULL
scheduled_at TIMESTAMPTZ DEFAULT now()
run_at TIMESTAMPTZ DEFAULT now()
started_at TIMESTAMPTZ NULL
completed_at TIMESTAMPTZ NULL
result JSONB NULL
error TEXT NULL
worker_id VARCHAR(100) NULL

-- job_attempts table (DLQ history)
id UUID PRIMARY KEY
job_id UUID REFERENCES jobs(id)
attempt INT
started_at TIMESTAMPTZ
failed_at TIMESTAMPTZ
error TEXT
worker_id VARCHAR(100)
```

---

## Start Command

You are now ready to build. Begin with Step 1:

```
Create a new branch feat/step-1-schema and build:
1. requirements.txt with all project dependencies
2. core/config.py — settings loaded from env vars (DATABASE_URL, REDIS_HOST, REDIS_PORT, API_KEY, PROJECT_ID, ENVIRONMENT)
3. migrations/ — Alembic setup with alembic.ini and env.py wired to DATABASE_URL
4. First Alembic migration: creates jobs and job_attempts tables exactly per the schema above, with indexes on (status, run_at), (worker_id, status), and (idempotency_key)

Use psycopg2 for sync DB access (workers and migrations). Follow the workflow rules: feature branch, expected output for every command, remind to merge when done.
```
