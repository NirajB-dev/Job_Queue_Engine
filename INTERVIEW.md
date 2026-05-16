# Interview Prep — Job Queue Engine

A deep-dive into every design decision, technology choice, tradeoff, and failure mode in this project. Written as a reference for system design interviews and technical conversations.

---

## Table of Contents

1. [Why This Project?](#1-why-this-project)
2. [System Design Overview](#2-system-design-overview)
3. [Technology Choices](#3-technology-choices)
4. [Architecture Decisions](#4-architecture-decisions)
5. [Reliability Deep-Dive](#5-reliability-deep-dive)
6. [Data Model Decisions](#6-data-model-decisions)
7. [API Design](#7-api-design)
8. [Observability](#8-observability)
9. [Testing Strategy](#9-testing-strategy)
10. [Infrastructure and CI/CD](#10-infrastructure-and-cicd)
11. [Failure Modes and Mitigations](#11-failure-modes-and-mitigations)
12. [What I Would Do Differently at Scale](#12-what-i-would-do-differently-at-scale)
13. [Interview Questions and Answers](#13-interview-questions-and-answers)

---

## 1. Why This Project?

Most portfolio projects demonstrate that you can call a framework. This project demonstrates that you can **design and reason about distributed systems** — the kind of problems that show up in senior engineering roles.

A job queue is a good choice because it forces you to think about:
- **Consistency vs. availability** (what happens when a worker crashes mid-job?)
- **Throughput vs. latency** (how do you process 100 jobs/s without blocking the enqueue path?)
- **Fairness vs. starvation** (what does "priority" actually mean under load?)
- **At-least-once vs. exactly-once** (and why the latter is almost always a lie)
- **Testability in distributed systems** (how do you write deterministic tests for a system with multiple concurrent actors?)

Every production system eventually needs something like this — background task processing, async workflows, retry logic, dead-lettering. Building it from scratch (rather than wrapping Celery or a managed service) forces you to understand what those systems actually do.

---

## 2. System Design Overview

### The Core Idea

The system separates three concerns:
1. **Enqueue** — accept a job, persist it, push to Redis: fast, stateless
2. **Process** — pull from Redis, execute, update Postgres: slow, stateful
3. **Manage** — scheduler promotes delayed jobs; watchdog reclaims lost jobs

This separation is what makes the system resilient. The API never blocks on job execution. Workers never need to talk to each other. The scheduler and watchdog are independent control-plane processes with no shared state beyond Redis and Postgres.

### Why Two Storage Systems?

**Redis is the queue; Postgres is the source of truth.**

Redis gives you:
- `BLPOP` — a blocking, atomic dequeue operation. No polling. Exactly the primitive you need for a work queue.
- `ZADD`/`ZRANGEBYSCORE` — a sorted set for delayed jobs, keyed by Unix timestamp. One query promotes all due jobs at once.
- Low latency for the hot path (enqueue/dequeue).

Postgres gives you:
- Durability: jobs survive Redis restarts, network splits, and OOM kills.
- Queryability: filter by status, type, priority, date range. Arbitrary analytics.
- Atomicity: `move_to_dlq` inserts into `job_attempts` and updates `jobs` in one transaction.
- Single source of truth: the `status` field in Postgres is what actually matters. Redis is just the delivery mechanism.

If Redis goes down, no new jobs are processed until it recovers — but no jobs are lost. They're all in Postgres as `pending`. A recovery script (or operator intervention) can re-enqueue them from the DB.

---

## 3. Technology Choices

### Python

**Why:** Python is the dominant language for data pipelines, ML workloads, and backend services at the companies that typically use job queues. The ecosystem (Celery, RQ, Dramatiq) is mature. For a portfolio project, it lets the design shine rather than the language.

**Tradeoff:** The GIL limits CPU parallelism within a process. For CPU-bound handlers, you'd need multiple worker processes (which we already do — each pod is a separate process). For I/O-bound handlers (API calls, DB queries), Python's asyncio or threads work fine. The job queue pattern naturally sidesteps this: each worker is its own process.

### FastAPI

**Why:** FastAPI gives you Pydantic validation, automatic OpenAPI docs, and dependency injection with essentially no boilerplate. The `lifespan` context manager is the right pattern for managing shared resources (DB connection, Redis client) in a single-process ASGI app.

**Alternative considered:** Flask. FastAPI wins on type safety, validation, and documentation generation. For a stateless API like this one, the async/sync distinction doesn't matter much — our handlers are all synchronous (psycopg2 is synchronous).

**Why not async psycopg2 / asyncpg?** The workers are synchronous Python. Mixing sync and async across the codebase (worker is sync, API is async) creates friction. psycopg2 + a sync FastAPI route works cleanly. At scale, you'd switch to asyncpg + a connection pool.

### Redis

**Why:** Redis was purpose-built for this use case. `BLPOP` is exactly the primitive you need — blocking, atomic, multi-key (for priority ordering). Alternatives:

| Option | Why not |
|--------|---------|
| RabbitMQ | Operationally heavier; AMQP adds complexity |
| Kafka | Consumer groups / offset management is overkill; designed for streaming, not task dispatch |
| AWS SQS | Managed, but visibility timeout model requires polling; no strict priority across queues |
| Postgres SKIP LOCKED | Viable (used by Que, GoodJob), but adds DB load and lacks the sorted-set primitive for delayed jobs |

For a single-region system under ~10K jobs/s, Redis is the right call. It's simple, fast, and has exactly the data structures you need.

### PostgreSQL

**Why:** The jobs table needs to be queryable (filter by status, paginate, aggregate). It needs transactions (DLQ move = two writes that must be atomic). It needs durability. NoSQL databases don't add value here — the schema is fixed and relational (jobs → attempts).

### psycopg2 (not SQLAlchemy ORM)

**Why raw SQL:** Every query in this codebase is intentional and visible. You can read `repository.py` and know exactly what SQL runs. No N+1 surprises, no lazy loading footguns, no magic.

The ORM abstraction is valuable when you have complex object graphs and want to defer SQL generation. For a system where every query is a known performance-critical path (dequeue, update status, claim stale), raw SQL is clearer and faster.

`RealDictCursor` gives you row-as-dict which maps directly to Pydantic `model_validate(dict(row))`. That's the only ORM feature we need.

### Alembic

**Why:** Schema migrations need to be tracked, versioned, and repeatable. `alembic upgrade head` in the CI/CD pipeline before deploying new app code ensures the DB schema and app are always in sync. The `DATABASE_URL` is injected from environment at runtime so no credentials live in `alembic.ini`.

### Pydantic + pydantic-settings

**Why:** Validation at the boundary. `CreateJobRequest` enforces that `max_attempts` is between 1 and 10, that `priority` is a valid enum value, that `run_at` is a valid datetime. FastAPI raises a 422 with a detailed error message before any DB write happens.

`pydantic-settings` reads env vars and `.env` files with the same validation. `settings.api_key` is a required string — if it's not set, the process fails at startup with a clear error message rather than silently using None.

---

## 4. Architecture Decisions

### Why Three Separate Processes (worker / scheduler / watchdog)?

Each process has a single responsibility and can be scaled and deployed independently.

| Process | Responsibility | Scaling |
|---------|----------------|---------|
| Worker | Execute handlers | Horizontal — add pods for throughput |
| Scheduler | Promote delayed jobs | Single replica — low load, no state |
| Watchdog | Reclaim stale workers | Single replica — scans periodically |

If you combined them into one process, a bug in the scheduler could bring down workers. Running the scheduler as a single replica prevents double-promotion of delayed jobs (two schedulers racing would both call ZADD→RPUSH, but `ZREM` is idempotent so the job would only land in the queue once — it's actually safe, just wasteful).

### Why BLPOP for Priority Instead of a Sorted Set?

The naive approach to priority queuing is a single sorted set with priority as the score. The problem: you can't block on a sorted set. You'd need to poll with `ZRANGEBYSCORE` on a loop, adding latency.

`BLPOP queue:high queue:normal queue:low 5` is atomic and blocking. Redis checks `queue:high` first; if empty, checks `queue:normal`; if empty, checks `queue:low`; if all empty, blocks for up to 5 seconds. This is a single round-trip with zero polling overhead.

**Starvation trade-off:** If HIGH is always non-empty, LOW jobs never run. This is intentional for our use case (HIGH jobs are truly urgent). For a fairer system, you'd implement weighted random sampling or a token bucket across queues. We document this trade-off explicitly.

### Why a Delayed ZSET Instead of Separate Delay Queues?

Redis sorted sets (`ZADD delayed:jobs {score: unix_timestamp} member`) give you a time-ordered structure that you can query efficiently: `ZRANGEBYSCORE delayed:jobs 0 {now}` returns all due jobs in a single command.

The scheduler runs every 1 second, pipelines the `RPUSH` + `ZREM` for all due jobs, and exits. This is O(n) in the number of due jobs, not O(total delayed jobs). At high volume, you'd batch or partition this.

**Alternative:** Store `run_at` in Postgres and have the scheduler poll with `SELECT * FROM jobs WHERE status='pending' AND run_at <= now()`. This works but is slower (DB query every second) and doesn't compose well with the Redis-first approach.

### Why Heartbeats Instead of a Visibility Timeout (SQS-Style)?

SQS uses a **visibility timeout**: when you dequeue a message, it becomes invisible for N seconds. If you don't delete it within N seconds, it reappears. No heartbeat needed.

We use **heartbeats** instead because:
1. Long-running jobs need a dynamic timeout. A job that takes 5 minutes shouldn't need a 5-minute visibility timeout set at enqueue time.
2. We want to know which worker is processing which job (for debugging, monitoring, and reclaim).
3. The heartbeat pattern is more explicit — a worker that is alive but stuck (deadlocked, thrashing GC) will stop sending heartbeats, allowing the watchdog to intervene.

**Downside:** More moving parts. The watchdog is a separate process that must be running. The heartbeat key in Redis has no TTL — if the watchdog dies, stale heartbeats accumulate. (Mitigated by the watchdog being a simple, low-risk process.)

### Why Injectable Dependencies (No Global State)?

```python
class Worker:
    def __init__(self, conn, redis_client, handler_registry):
        ...
```

Every major component takes its dependencies as constructor arguments. No `import global_db; global_db.query(...)`. This matters for:

1. **Testing:** Inject a real test DB and test Redis. No mocking. No monkey-patching.
2. **Flexibility:** The API injects DB/Redis from `app.state` (one connection per process). The worker creates its own connection. The test suite uses its own isolated connection.
3. **Clarity:** Reading a class tells you everything it depends on.

### Why Extract `_tick()` from Run Loops?

```python
# Scheduler
def _tick(self) -> int:
    return self._queue.promote_delayed()

def run(self) -> None:
    while self._running:
        self._tick()
        time.sleep(1)
```

`_tick()` is the testable unit. Tests call `_tick()` directly — no need to run the infinite loop, no need to mock `time.sleep`, no need to deal with threading. The test for "watchdog reclaims stale jobs" is 10 lines of straightforward code.

This pattern appears in every process: worker, scheduler, watchdog. It's a small design choice that makes the difference between a testable and an untestable system.

---

## 5. Reliability Deep-Dive

### The At-Least-Once Delivery Proof

Claim: every job that enters the system will be executed at least once unless it is explicitly cancelled or exhausts `max_attempts`.

**Case 1: Worker crashes before picking up the job.**
Job is in Redis list. Redis is persistent (we use AOF/RDB). Job stays in list until another worker calls BLPOP. ✓

**Case 2: Worker picks up the job, crashes before writing RUNNING to Postgres.**
Job is in neither Redis (dequeued) nor Postgres (still PENDING). This is a loss window.

Mitigation: the watchdog monitors heartbeats. When the worker starts, it immediately writes its heartbeat. If it crashes before updating Postgres, the watchdog sees a stale heartbeat. However, `claim_stale_jobs` only reclaims jobs with `status=running`. A job stuck in PENDING with no worker is never lost — the BLPOP will just never happen again.

**Actual risk:** Between `BLPOP` and `update_status(RUNNING)`, the job is in a grey zone. If the worker dies in this window (which is milliseconds), the job is orphaned. At production scale, this is handled by adding a "processing" status or using Redis's `RPOPLPUSH` (atomic dequeue + push to a processing list) so orphaned jobs can be found by scanning the processing list. This is a known gap in the current design.

**Case 3: Worker picks up the job, writes RUNNING, crashes during execution.**
Watchdog detects stale heartbeat (worker not sending heartbeats) → calls `claim_stale_jobs(worker_id)` → job reset to PENDING, worker_id cleared → job re-enqueued → next worker picks it up. ✓

**Case 4: Worker finishes the job, crashes before writing DONE to Postgres.**
Job is re-executed on the next worker. This is the "at least once" guarantee — the handler may run twice. Handlers should be idempotent if possible.

**Case 5: Postgres is down.**
Workers cannot update job status. They will log the error and the job remains RUNNING in Postgres with a stale heartbeat. When Postgres recovers, the watchdog reclaims jobs whose workers have died. Jobs that were being processed when Postgres went down may be re-executed.

### Retry Backoff Formula

```python
delay = min(10 × 2^attempt + U(0, 5), 3600)
```

- Attempt 1: ~10–15s
- Attempt 2: ~20–25s
- Attempt 3: ~40–45s
- Attempt 4: ~80–85s
- Attempt 7: ~1280s (~21min)
- Capped at 3600s (1 hour)

**Why jitter?** Without jitter, all jobs that fail simultaneously retry at the same time, creating a thundering herd that can overwhelm a recovering downstream service. Jitter spreads retries across the delay window. This is the same pattern used by AWS SDK exponential backoff.

**Why 10 as the base?** Long enough that a transient failure (network blip, DB overload) has time to recover before the first retry. Short enough that you don't wait 10 minutes for a fast-failing job.

### Idempotency at the DB Level

```sql
idempotency_key VARCHAR(255) UNIQUE
```

The `UNIQUE` constraint is enforced by Postgres, not by the application. This means no TOCTOU race condition: two concurrent requests with the same key cannot both succeed. One will get a `UniqueViolation` exception, which the repository catches and re-raises, and the API translates to a 409.

**Why not check-then-insert?** A `SELECT WHERE idempotency_key = ?` followed by `INSERT` has a race condition — two concurrent requests both see no row, both try to insert, one fails. The constraint is the atomic solution.

---

## 6. Data Model Decisions

### Why `status` as VARCHAR(20) Not an Enum?

Postgres enum types are hard to migrate (you need an ALTER TYPE to add a value). VARCHAR with a CHECK constraint (or application-level validation) is easier to evolve. Pydantic's `JobStatus` class provides the compile-time safety that the DB doesn't enforce.

### Why Separate `job_attempts` Table?

The `jobs` table stores the current state of a job. The `job_attempts` table stores the history of every terminal failure. This separation means:

1. The `jobs` table stays lean (one row per job, fixed size)
2. DLQ history is queryable: `SELECT * FROM job_attempts WHERE job_id = ?`
3. You can truncate `job_attempts` for cleanup without touching `jobs`

**Alternative:** Store attempts as a JSON array in `jobs.attempts_history`. Simpler schema but harder to query and grows unbounded in the row.

### Why `scheduled_at` and `run_at` Both?

- `scheduled_at`: when the job was created. Immutable.
- `run_at`: when the job should first be eligible for processing. Mutable — updated by the retry logic on each reschedule.

This means you can always see when a job was originally submitted (`scheduled_at`) and when it's next eligible to run (`run_at`). The `(status, run_at)` index supports efficient worker polling: `WHERE status='pending' AND run_at <= now()`.

### Why Not Store the Worker's UUID in Redis?

Worker IDs are UUIDs generated at startup: `worker-{uuid4()}`. They're stored in:
- Redis: `SET worker:heartbeat:{worker_id} {timestamp}` — for liveness detection
- Postgres: `jobs.worker_id` — for tracing which worker processed which job

The heartbeat key name encodes the worker ID. The watchdog reads all `worker:heartbeat:*` keys, extracts the worker ID from the key name, and checks the timestamp. No secondary lookup needed.

---

## 7. API Design

### Why `X-API-Key` Instead of JWT / OAuth?

For a service-to-service API, a shared secret is the simplest auth mechanism with the right security properties. The API is not user-facing; clients are trusted services.

JWT adds complexity (signing, expiry, refresh) without benefit when the token audience is a fixed set of internal services. OAuth adds even more complexity (authorization server, token exchange).

**In production:** you'd rotate the API key via Secret Manager without redeploying the service (Secret Manager supports version rotation with version aliases). For user-facing APIs, OAuth/OIDC would be the right choice.

### Why `POST /jobs/{id}/cancel` and Not `DELETE /jobs/{id}`?

HTTP semantics: `DELETE` implies the resource is gone. A cancelled job still exists in Postgres with `status=dead`. It's visible in audit logs, re-queuable, and queryable. `POST /cancel` is the correct verb for a state transition action.

This is the same pattern used by Stripe (`POST /subscriptions/{id}/cancel`) and GitHub (`POST /repos/{owner}/{repo}/actions/runs/{run_id}/cancel`).

### Why Paginate with `limit`/`offset` Not Cursor-Based?

For this use case (admin dashboard, small-to-medium result sets), `limit`/`offset` is simpler and sufficient. Cursor-based pagination is more correct for large result sets with frequent inserts (offset 50 on a live queue means different rows each time), but the complexity cost is high.

At scale: switch to cursor-based pagination keyed on `(run_at, id)`.

### The `/prometheus` Endpoint Has No Auth

Prometheus scrapers typically run inside the same network and don't support custom headers. The Prometheus scrape endpoint is conventionally unauthenticated and protected by network policy instead. The business-logic `/metrics` endpoint keeps the `X-API-Key` requirement.

---

## 8. Observability

### Three Tiers

1. **Structured logging** — every significant event (job started, job done, job failed, worker reclaimed) is logged with job ID, type, worker ID, and timing. In GKE, these logs flow to Cloud Logging.

2. **Prometheus metrics** — seven metrics covering queue depth, delayed count, jobs by status, throughput rate, and execution latency. The `GET /prometheus` endpoint updates gauges on each scrape (pull model), ensuring Prometheus always has fresh data.

3. **Grafana dashboard** — six panels importable in one click. Queue depth by priority (stacked area), delayed jobs stat, DLQ depth stat, jobs by status time series, throughput rate (processed/s, failed/s), and job duration percentiles (p50/p95/p99).

### Why Pull Model for Gauges?

Prometheus's native model is pull: it scrapes your endpoint periodically. For gauges (current queue depth, current job count), the freshest data is always the right answer — querying Redis and Postgres on each scrape ensures no stale data. For counters (jobs_processed_total), the worker increments them in-process and the total accumulates across scrapes.

**Multi-process caveat:** In production, Prometheus would scrape both the API pod (for gauges) and each worker pod (for counters/histograms). Each worker process has its own in-process prometheus_client registry. The current setup correctly instruments the code — deploying with a Prometheus `kubernetes_sd_configs` pointing at the worker pods would give you the full picture.

---

## 9. Testing Strategy

### Philosophy: Integration Tests Over Unit Tests (With One Exception)

For a system whose correctness depends on interactions between components (API → Redis → Postgres → worker), unit tests with mocks verify implementation details, not behavior. If you mock psycopg2, you might pass tests but fail on a real DB with a transaction isolation issue.

Every test in this project runs against a real Postgres and Redis instance. This is slower than unit tests but catches real bugs: transaction isolation problems, Redis key expiry, BLPOP timeout behavior.

**The exception:** `test_retry.py` tests the pure `backoff_seconds()` function with no I/O. Unit test is appropriate — the function has no dependencies.

### Three Test Layers

**Layer 1 — Component integration tests (CI):** 90 tests across repository, queue, worker, scheduler, watchdog, API, Prometheus endpoint. Run in GitHub Actions against ephemeral Postgres/Redis service containers. Fast (~30s), fully automated.

**Layer 2 — End-to-end correctness tests (local):** 11 tests in `tests/chaos/test_correctness.py`. Hit the real API, exercise real workers, verify real DB state. Tests idempotency, DLQ promotion, retry mechanics, priority ordering, and delayed job scheduling against the full Docker Compose stack. Slower (~36s) because they wait for workers to process jobs.

**Layer 3 — Chaos tests (local):** 5 tests in `tests/chaos/test_crash.py`. Pause workers mid-job, verify watchdog reclaims within 120s. Stop the scheduler, verify delayed job is promoted after restart. These take ~3 minutes and require Docker access. They're excluded from CI and run manually before major releases.

### The `_tick()` Pattern and Testability

The biggest testing challenge in distributed systems is the infinite loop. The watchdog's `run()` method loops forever. Testing it directly requires threading, timeouts, and non-determinism.

Solution: extract all the logic into `_tick()` which runs one cycle and returns. Tests call `_tick()` and assert on the result synchronously. No threads, no sleeps, no timeouts. This is the single most impactful testing decision in the codebase.

### Test Isolation

Each test module has an `autouse=True` fixture that:
1. Calls `conn.rollback()` (clears any aborted transaction from a previous test)
2. Deletes all rows from `job_attempts` then `jobs` (in correct FK order)
3. Calls `redis_client.flushdb()` (clears all Redis state)

This ensures tests are fully independent regardless of execution order. The rollback-before-delete pattern is critical: if a previous test left the connection in an ABORTED state (e.g., from a caught UniqueViolation), psycopg2 refuses to execute the DELETE without an explicit rollback first.

---

## 10. Infrastructure and CI/CD

### Why GKE Autopilot for Workers, Cloud Run for API?

**Workers:** Long-running processes that block on BLPOP. They need persistent connections to Postgres and Redis. Cloud Run's billing model (per-request, scales to zero) is wrong for this — you'd pay nothing while idle but lose connection state on every cold start. GKE Autopilot manages node provisioning automatically while giving you persistent pod semantics.

**API:** Stateless, request-driven, scales to zero gracefully. Cloud Run is the right abstraction — no node management, built-in HTTPS, automatic scaling. The lifespan context manager handles connection setup/teardown per container instance.

### Why Workload Identity Federation?

No long-lived credentials in GitHub. WIF lets GitHub Actions exchange a short-lived OIDC token for a GCP access token without storing service account keys. Keys are a secret management problem — they expire, leak, and need rotation. OIDC tokens are ephemeral and bound to the specific workflow run.

### The Migrate Job Pattern

The `migrate-job` Cloud Run Job runs `alembic upgrade head` as a one-shot container. The CD pipeline uses `create || update` to make it idempotent: first run creates, subsequent runs update the image tag. The job runs before the API deploys, ensuring the schema is always ahead of the code.

**Why not run migrations in the API startup?**
1. Race condition: multiple API pods might run migrations simultaneously.
2. The API SA would need schema-modification permissions (high privilege).
3. Startup time is penalized for every API restart, not just schema changes.

### Why the API Runs as `job-queue-sa` Not the Default Compute SA?

The default Compute Engine SA has broad read access to many GCP APIs. `job-queue-sa` has exactly the permissions it needs: `secretmanager.secretAccessor`, `cloudsql.client`, `artifactregistry.reader`. Principle of least privilege — a compromised API container has limited blast radius.

---

## 11. Failure Modes and Mitigations

| Failure | What Happens | Mitigation |
|---------|-------------|------------|
| Worker crashes mid-job | Job stays RUNNING in Postgres | Watchdog reclaims after 60s+30s |
| Redis goes down | No new jobs dequeued; API enqueue fails | Workers retry BLPOP; jobs safe in Postgres |
| Postgres goes down | Workers can't update status; API returns 503 | Jobs re-executed on recovery (at-least-once) |
| Scheduler dies | No delayed jobs promoted | Delayed jobs queue up; resume when scheduler restarts |
| Watchdog dies | Stale workers not reclaimed | RUNNING jobs pile up; restart watchdog |
| Handler panics (uncaught exception) | Job moves to failed/DLQ | try/except in worker._process(), logged |
| Bad job type submitted | Immediate DLQ (no retry) | "No handler" error, attempts=1, status=dead |
| Duplicate idempotency key | 409 Conflict returned to caller | UNIQUE constraint in Postgres |
| Redis memory full | RPUSH fails | Monitor `job_queue_depth` gauge; add Redis capacity |
| k8s pod eviction mid-job | Same as worker crash | Watchdog reclaims after threshold |

---

## 12. What I Would Do Differently at Scale

### At 10× current load (~1,000 jobs/s)

1. **Connection pooling** — Replace `psycopg2.connect()` per worker with `psycopg2.pool.ThreadedConnectionPool` or `pgbouncer` as a sidecar. Each worker currently holds one connection; at 10 workers × 2 replicas that's fine, but at 50 workers you hit Postgres connection limits.

2. **Redis Cluster** — A single Redis node is a bottleneck and single point of failure. Redis Cluster shards across multiple nodes. The queue keys would need a hash tag (`{queue}:high`) to keep all priority keys on the same shard.

3. **Metrics aggregation** — Add a pushgateway or use prometheus_client multiprocess mode so worker pod metrics aggregate correctly in Prometheus.

### At 100× current load (~10,000 jobs/s)

4. **Kafka for durable streaming** — At this scale, Redis becomes a bottleneck. Kafka's consumer group model with manual offset commits gives you per-partition ordering and durable delivery without a separate watchdog (Kafka handles redelivery on consumer crash).

5. **Partitioned workers** — Route job types to specific worker pools. High-throughput jobs go to dedicated workers with optimized handler code.

6. **Outbox pattern for enqueue** — Instead of writing to Postgres then Redis in two separate calls (with a failure window between them), write a job row to Postgres with a `status=pending` and have a CDC (Change Data Capture) process stream new rows to Redis. This closes the gap between DB write and Redis enqueue.

7. **Exactly-once semantics** — Use a distributed transaction (Postgres + Redis via a Lua script that checks a processed-jobs set) or idempotent handlers with a deduplication key. True exactly-once is expensive; at high scale you design handlers to be idempotent instead.

### Architecture I Would Remove

The separate `scheduler` and `watchdog` processes are correct but operationally annoying — three extra deployments to manage. At scale, their responsibilities could be absorbed by the workers themselves (each worker runs a background goroutine/thread for heartbeating and scheduled promotion checks), or delegated to Kubernetes CronJobs.

---

## 13. Interview Questions and Answers

### "Walk me through what happens when a job is submitted."

1. Client `POST /jobs` with `type`, `payload`, optional `priority`, `run_at`, `idempotency_key`.
2. API validates the request (Pydantic). If `idempotency_key` is provided and already exists, return 409.
3. API calls `repo.create()` → `INSERT INTO jobs ... RETURNING *`. On `UniqueViolation`, rollback and return 409.
4. API calls `queue.enqueue(job.id, priority, run_at)`. If `run_at <= now()`, `RPUSH queue:{priority} job_id`. If future, `ZADD delayed:jobs {run_at} job_id:{priority}`.
5. Return 201 with the created job.

### "How do you guarantee a job runs at least once?"

Three layers:
1. **Postgres durability** — The job is written to Postgres before Redis. If Redis loses the job, we can re-enqueue from Postgres.
2. **BLPOP atomicity** — The dequeue is atomic. A job is either in the queue or being processed. No partial state.
3. **Watchdog** — If the processing worker dies (crash, OOM, node eviction), its heartbeat goes stale. The watchdog detects this, resets the job to pending, and re-enqueues it.

The gap: between `BLPOP` and `UPDATE status=running`, the job is in neither Redis nor Postgres as "in progress." A crash here loses the job. Mitigation: use `RPOPLPUSH` (dequeue + push to a processing list atomically) and have the watchdog scan the processing list for orphans.

### "What is the difference between at-least-once and exactly-once delivery?"

**At-least-once:** Every job runs at least one time. Under failure (worker crash), a job may run multiple times. Requires idempotent handlers.

**Exactly-once:** Every job runs exactly one time. Requires distributed coordination (e.g., a two-phase commit between Redis and Postgres, or Kafka transactions). Extremely expensive; most production systems avoid it by making handlers idempotent.

**Effectively exactly-once:** Combine at-least-once delivery with idempotent handlers. If the handler's effect is idempotent (e.g., `INSERT OR IGNORE`, `SET field = value`), running it twice is safe.

### "Why not use Celery?"

Celery is a mature, production-proven framework. For a new project, you'd absolutely consider it. But:

1. Celery's configuration surface is enormous. There are 200+ configuration options and multiple broker/backend combinations with different consistency guarantees.
2. Celery's at-least-once guarantees depend on the broker configuration. With Redis as broker, it's easy to accidentally configure it wrong.
3. Building from scratch demonstrates understanding of the underlying primitives. In an interview context, saying "I used Celery" doesn't show understanding of BLPOP, visibility timeouts, or heartbeats.
4. This system is intentionally simple. 1,500 lines of Python with no hidden magic is easier to debug and reason about than a large framework.

### "How does the priority queue work under load?"

`BLPOP queue:high queue:normal queue:low 5` is Redis's answer to priority queues. Redis processes this as: pop from `queue:high` if non-empty; else pop from `queue:normal` if non-empty; else pop from `queue:low`. This is atomic and O(1).

Under sustained HIGH load, LOW jobs starve. This is intentional — the queue is designed for cases where HIGH jobs are genuinely more important. For fairer queuing, implement weighted random sampling: with probability 0.7 pop from HIGH, 0.25 from NORMAL, 0.05 from LOW. This requires multiple BLPOP calls (one per queue) which loses the atomic single-call guarantee.

### "What happens if two schedulers run simultaneously?"

Both call `ZRANGEBYSCORE delayed:jobs 0 now()` — they see the same set of due jobs. Both call `RPUSH queue:{priority} job_id` and `ZREM delayed:jobs member`. The first `ZREM` succeeds and removes the member. The second `ZREM` on a non-existent member is a no-op (returns 0). The `RPUSH` happens twice — the job is in the queue twice.

**Mitigation:** Use a Lua script to make `ZRANGEBYSCORE` + `ZREM` + `RPUSH` atomic. Or use `BLMOVE` (if Redis 6.2+). Or ensure only one scheduler runs (Kubernetes ensures a Deployment with 1 replica doesn't run two simultaneously via leader election — but K8s doesn't guarantee exactly one at rollout boundaries).

In practice, a duplicate in the queue means the handler runs twice — which is why idempotent handlers matter.

### "How do you test distributed systems?"

Three approaches in this project:

1. **Extract the loop body** (`_tick()` pattern) — test the unit of work without the loop, eliminating non-determinism from timing.

2. **Use real dependencies, not mocks** — real Postgres, real Redis. Mocks verify implementation details; real dependencies verify behavior.

3. **Docker Compose + container lifecycle** — chaos tests use `docker compose pause worker` to simulate a crash, then assert on observable state (job status in Postgres via the API). This tests the full system, including the watchdog reclaim path, end-to-end.

### "What's your biggest single improvement to this system?"

Close the BLPOP→UPDATE gap. Today, between `BLPOP` (job removed from Redis) and `UPDATE status=running` (job marked in Postgres), a crash loses the job permanently.

The fix: use `RPOPLPUSH queue:{priority} queue:processing:{worker_id}`. This atomically moves the job from the live queue to a per-worker processing queue. The watchdog scans `queue:processing:*` keys for workers with stale heartbeats and moves those jobs back to the live queue. This closes the gap at the cost of one extra Redis key per worker.
