**🔨 BUILD PROJECT**

**Distributed Job Queue & Task Processing Engine**

Backend Engineering · Async Systems · Reliability Patterns

Python · Redis · PostgreSQL · FastAPI · Docker · Prometheus · k6

Individual Project Specification · Niraj Bharambe · May 2026

# **Why This Project**

| **Backend Eng - Supporting** | **SDE Roles** | **Systems Design Interviews** |
| ---------------------------- | ------------- | ----------------------------- |

Async processing patterns, failure handling, and queue design are probed in almost every backend engineering interview. This project lets you answer canonical questions from first principles - not just 'I've used Celery' but 'here is how priority queuing, heartbeat-based job reclaim, and dead-letter queues work at the implementation level.' It also introduces zero overlap with Beacon, which already covers REST APIs, PostgreSQL, Redis caching, and microservices.

# **Architecture Overview**

| **System Components**                                                                                                                          |
| ---------------------------------------------------------------------------------------------------------------------------------------------- |
| ▸ FastAPI control plane - job submission, inspection, cancellation, and metrics endpoints                                                      |
| ▸ Redis - hot queue (sorted sets for priority ordering), delayed job scheduling (ZSET by run_at timestamp), worker presence tracking           |
| ▸ PostgreSQL - durable job state (source of truth), dead-letter records with full attempt history                                              |
| ▸ Worker pool - N concurrent worker processes/threads pulling from Redis via BLPOP, executing job handlers, writing results back to PostgreSQL |
| ▸ Scheduler - lightweight thread/process that polls Redis ZSET for delayed jobs ready to promote to the active queue                           |
| ▸ Watchdog - monitors worker heartbeat keys in Redis; reclaims orphaned jobs from crashed workers                                              |
| ▸ Prometheus metrics endpoint + Grafana dashboard for real-time observability                                                                  |

# **Data Model**

## **Job Schema (PostgreSQL)**

jobs table:

id UUID PRIMARY KEY DEFAULT gen_random_uuid()

type VARCHAR(100) NOT NULL -- handler name

payload JSONB NOT NULL -- arbitrary job data

priority SMALLINT DEFAULT 1 -- 0=HIGH 1=NORMAL 2=LOW

status VARCHAR(20) DEFAULT 'pending' -- pending|running|done|failed|dead

attempts INT DEFAULT 0

max_attempts INT DEFAULT 3

idempotency_key VARCHAR(255) UNIQUE NULL -- optional dedup key

scheduled_at TIMESTAMPTZ DEFAULT now()

run_at TIMESTAMPTZ DEFAULT now() -- delayed if future

started_at TIMESTAMPTZ NULL

completed_at TIMESTAMPTZ NULL

result JSONB NULL

error TEXT NULL

worker_id VARCHAR(100) NULL -- which worker is running it

job_attempts table (DLQ history):

id UUID PRIMARY KEY

job_id UUID REFERENCES jobs(id)

attempt INT

started_at TIMESTAMPTZ

failed_at TIMESTAMPTZ

error TEXT

worker_id VARCHAR(100)

# **Build Steps - Ordered by Dependency**

| **1**  | **PostgreSQL Schema & Migrations**<br><br>Create jobs and job_attempts tables. Write Alembic (Python) migration scripts. Seed with sample jobs of each priority and status for local testing.                                                                                                                                                                           |
| ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **2**  | **Job Model & Repository Layer**<br><br>Python dataclass or Pydantic model for Job. JobRepository class with: create(), get_by_id(), list(filters), update_status(), move_to_dlq(), requeue_from_dlq(). All DB access goes through this layer - no raw SQL in workers or API handlers.                                                                                  |
| **3**  | **Redis Queue Layer**<br><br>RedisQueue class wrapping redis-py. Methods: enqueue(job_id, priority, run_at), dequeue_blocking(timeout=5) via BLPOP on priority lists, promote_delayed() for scheduler, register_worker_heartbeat(worker_id), get_stale_workers(threshold_seconds).                                                                                      |
| **4**  | **Worker & Handler Registry**<br><br>HandlerRegistry: dict mapping job.type string to a Python callable. Worker loop: BLPOP → look up handler → execute → write result/error → update PostgreSQL. Each worker has a unique worker_id (UUID). Wrap handler execution in try/except - on exception, increment attempts, compute backoff delay, re-enqueue or move to DLQ. |
| **5**  | **Retry Logic with Backoff**<br><br>On job failure: if attempts &lt; max_attempts → compute delay = min(base \* 2^attempts + jitter, max_delay) → set run_at = now() + delay → re-enqueue to Redis delayed ZSET. If attempts &gt;= max_attempts → status = 'dead' → INSERT into job_attempts → PostgreSQL only (DLQ).                                                   |
| **6**  | **Watchdog Thread**<br><br>Background thread running every 30s. Queries Redis for worker heartbeat keys older than threshold (e.g. 60s). For each stale worker: find jobs with worker_id = stale_worker AND status = 'running' → re-enqueue them. Log a warning with worker_id and reclaimed job IDs.                                                                   |
| **7**  | **Scheduler Thread**<br><br>Background thread running every 1s. Calls ZRANGEBYSCORE on the delayed ZSET with max=now(). For each due job_id: LPUSH to the appropriate priority queue → ZREM from delayed ZSET. Atomic via Redis pipeline.                                                                                                                               |
| **8**  | **FastAPI Control Plane**<br><br>POST /jobs, GET /jobs/{id}, GET /jobs, POST /jobs/{id}/cancel, POST /jobs/{id}/requeue (from DLQ), GET /workers, GET /metrics. Input validation via Pydantic. Auth: simple API key header (X-API-Key) checked against env variable.                                                                                                    |
| **9**  | **Prometheus Metrics**<br><br>Instrument: queue_depth{priority} gauge, dlq_depth gauge, jobs_processed_total counter, jobs_failed_total counter, job_duration_seconds histogram (buckets: .1 .5 1 5 10 30). Expose via /metrics endpoint using prometheus-client library.                                                                                               |
| **10** | **Docker Compose**<br><br>Services: api (FastAPI), worker (N replicas via --scale), scheduler (1 instance), watchdog (1 instance), postgres, redis. Single docker compose up --build starts everything. README shows exactly how to scale workers: docker compose up --scale worker=5.                                                                                  |
| **11** | **Load Test & Grafana**<br><br>k6 script: ramp to 100 virtual users enqueuing jobs, measure throughput and p99 enqueue latency. Grafana dashboard: queue depth over time, throughput, p50/p99 job execution latency, DLQ depth, worker count. Capture screenshots for README.                                                                                           |
| **12** | **Chaos & Correctness Tests**<br><br>Chaos: docker compose stop worker mid-job; assert job is reclaimed within 90s. Correctness: pytest suite covering priority ordering (HIGH always before LOW), retry backoff timing, DLQ promotion at max_attempts, idempotency key deduplication, graceful shutdown (SIGTERM drains in-flight jobs).                               |

# **Priority Queue Design - Redis Implementation**

Three separate Redis lists: queue:high, queue:normal, queue:low. Workers call BLPOP queue:high queue:normal queue:low 5 - Redis returns the first available item from the highest-priority non-empty list, blocking up to 5 seconds. This gives strict priority ordering with a single atomic BLPOP call - no polling, no starvation logic needed for HIGH and NORMAL. LOW jobs may starve if HIGH is always full; document this trade-off and optionally add a starvation counter that promotes LOW jobs after N seconds of waiting.

# **Retry Backoff Formula**

Base delay: 10 seconds. Formula: delay = min(10 \* 2^attempt + random(0, 5), 3600). Attempt 1: ~10-15s. Attempt 2: ~20-25s. Attempt 3: ~40-45s. Cap at 1 hour. Jitter (random 0-5s) prevents retry storms when many jobs fail simultaneously - without jitter, all retries fire at the same second and overwhelm the system again.

# **Key Metrics to Hit**

| **3 priority**             | **DLQ + requeue**          | **At-least-once**  | **<5s reclaim**      | **k6 tested**       | **Prometheus**         |
| -------------------------- | -------------------------- | ------------------ | -------------------- | ------------------- | ---------------------- |
| HIGH / NORMAL / LOW queues | Full dead-letter lifecycle | Delivery guarantee | Watchdog reclaim SLA | 10k jobs under load | Full metrics + Grafana |

# **Interview Talking Points**

| **Canonical Questions - Answer from This Project**                                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ▸ How do you implement priority queuing at scale? - Three Redis lists + BLPOP with ordered key list. Single atomic dequeue operation gives strict priority without a custom heap.                                                                                                                                                                       |
| ▸ How do you guarantee at-least-once delivery without a message broker? - Visibility timeout via heartbeat. Job status is 'running' in PostgreSQL. Worker updates heartbeat in Redis every 15s. Watchdog reclaims jobs from workers whose heartbeat is > 60s stale.                                                                                     |
| ▸ How do you handle retry storms? - Exponential backoff with jitter. Without jitter, all retries from a mass failure fire simultaneously and overwhelm the system again. Jitter spreads them across a window.                                                                                                                                           |
| ▸ What's the difference between at-least-once and exactly-once delivery? - At-least-once: job may run more than once (our model). Exactly-once requires distributed transactions or idempotency at the handler level. We support idempotency keys on enqueue (Redis SETNX prevents duplicate queuing) but handlers must still be idempotent themselves. |
| ▸ How would you scale this horizontally? - Workers are stateless - add replicas via Docker Compose --scale or Kubernetes HPA. Redis is the coordination layer (no shared worker state). PostgreSQL handles concurrent writes via row-level locking on job status updates.                                                                               |
| ▸ What happens if Redis goes down? - Jobs already dequeued but not completed are lost from the queue (at-least-once not guaranteed during Redis downtime). Mitigation: persist job_id to Redis queue atomically with PostgreSQL INSERT using a two-phase approach, or use a Redis replica with persistence (AOF).                                       |
| ▸ How does the DLQ work? - Jobs exceeding max_attempts are set to status='dead' and their attempt history is written to job_attempts. The DLQ is queryable via GET /jobs?status=dead. Operators can requeue individual jobs via POST /jobs/{id}/requeue after fixing the underlying issue.                                                              |

# **Folder Structure**

job-queue/

├── api/

│ ├── main.py # FastAPI app, routers

│ ├── routers/

│ │ ├── jobs.py # CRUD + cancel + requeue

│ │ ├── workers.py # worker list endpoint

│ │ └── metrics.py # Prometheus /metrics

│ └── middleware/ # API key auth, request logging

├── core/

│ ├── models.py # Job Pydantic model

│ ├── repository.py # JobRepository (PostgreSQL)

│ ├── queue.py # RedisQueue abstraction

│ └── registry.py # HandlerRegistry

├── worker/

│ ├── worker.py # Worker loop + BLPOP

│ ├── handlers/ # Job type implementations

│ └── retry.py # Backoff logic

├── scheduler/

│ └── scheduler.py # Delayed job promotion

├── watchdog/

│ └── watchdog.py # Heartbeat monitoring

├── migrations/ # Alembic SQL migrations

├── tests/

│ ├── test_priority.py

│ ├── test_retry.py

│ ├── test_dlq.py

│ ├── test_idempotency.py

│ └── test_chaos.py # Kill worker, assert reclaim

├── k6/

│ └── load_test.js # 10k job enqueue ramp

├── grafana/

│ └── dashboard.json # Importable Grafana dashboard

├── docker-compose.yml

└── README.md

# **Resume Bullets**

- Built a distributed job queue engine in Python with priority-based Redis queues (HIGH/NORMAL/LOW via BLPOP), configurable worker pools, and at-least-once delivery guaranteed via heartbeat-based job reclaim on worker crash
- Implemented per-job retry logic with exponential backoff and jitter (delay = 10 \* 2^attempt + jitter, cap 1hr) to prevent retry storms; dead-letter queue for exhausted jobs with full attempt history and a requeue API
- Instrumented with Prometheus (queue depth, throughput, p99 latency, DLQ depth); k6 load tested at 10,000 jobs across concurrent workers, validating priority ordering and graceful SIGTERM shutdown
- Designed delayed job scheduling via Redis ZSET promotion and idempotency key deduplication on enqueue; chaos tested by killing workers mid-job and asserting reclaim within 90 seconds

# **Stack Reference**

| **Language**      | Python 3.11+                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------- |
| **Queue**         | Redis 7 - sorted sets (ZSET) for delayed jobs, lists for priority queues, BLPOP for workers |
| **Database**      | PostgreSQL 15 - durable job state, DLQ, attempt history; Alembic migrations                 |
| **API**           | FastAPI, Pydantic v2, Uvicorn                                                               |
| **Observability** | Prometheus (prometheus-client), Grafana dashboard                                           |
| **Load testing**  | k6                                                                                          |
| **Infra**         | Docker, Docker Compose (multi-service with --scale for workers)                             |
| **Testing**       | pytest, pytest-asyncio, coverage.py                                                         |
| **Libraries**     | redis-py, asyncpg or psycopg2, tenacity (optional), structlog                               |