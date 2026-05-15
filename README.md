# Job Queue Engine

Distributed job queue and task processing engine built on Python, Redis, PostgreSQL, and Kubernetes — deployed on GCP.

## Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Cloud Run |
| Workers | GKE Autopilot + HPA |
| Queue | Memorystore (Redis 7) |
| Database | Cloud SQL (PostgreSQL 15) |
| Observability | Google Managed Prometheus + Grafana |
| IaC | Terraform |
| Load testing | k6 |

## Architecture

```
Cloud Run (FastAPI)
    ├── Memorystore Redis  ← priority queues + delayed ZSET + heartbeats
    └── Cloud SQL Postgres ← durable job state + DLQ + attempt history

GKE Autopilot
    ├── worker (HPA, scales on queue depth)
    ├── scheduler (1 replica — promotes delayed jobs)
    └── watchdog (1 replica — reclaims orphaned jobs)
```

## Project Structure

```
job-queue/
├── api/            # FastAPI control plane
├── core/           # Models, repository, Redis queue, handler registry
├── worker/         # Worker loop + job handlers + retry logic
├── scheduler/      # Delayed job promotion thread
├── watchdog/       # Heartbeat monitoring + job reclaim
├── migrations/     # Alembic DB migrations
├── k8s/            # Kubernetes manifests
├── infra/terraform # GCP infrastructure as code
├── tests/          # pytest suite (priority, retry, DLQ, chaos)
├── k6/             # Load test scripts
└── grafana/        # Dashboard JSON
```

## Quick Start

See [infra/terraform/README.md](infra/terraform/README.md) for GCP setup.
