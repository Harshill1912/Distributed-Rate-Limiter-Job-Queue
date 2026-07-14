# Distributed Job Queue & Rate Limiter

A backend system that demonstrates the core building blocks of large-scale services:
an **atomic distributed rate limiter** and a **reliable, at-least-once job queue** with
retries, backoff, a dead-letter queue, and a live monitoring dashboard.

Built with **FastAPI · Redis · PostgreSQL · Docker**, and load-tested with **Locust**.

![CI](https://github.com/Harshill1912/Distributed-Rate-Limiter-Job-Queue/actions/workflows/ci.yml/badge.svg)

---

## Why this project

It's small on purpose, but it exercises the ideas that come up in real systems and in
system-design interviews:

- **Token-bucket rate limiting** done *atomically* (no race conditions under load)
- **At-least-once delivery** with crash recovery (no lost jobs)
- **Idempotency** (duplicate submissions are rejected)
- **Retries with exponential backoff** + a **dead-letter queue**
- **Observability** — a real-time dashboard + `/api/metrics` with **p50/p95/p99 latency**
- Containerised, tested, and CI-checked

## Architecture

```
                          ┌──────────────────────────────────────────┐
   HTTP client ─────────▶ │            FastAPI  (app/main.py)         │
   (dashboard / curl)     │  /check/rate_limit/{user}   → rate limiter│
                          │  /api/submit_custom_job     → producer    │
                          │  /api/metrics, /api/jobs, /dashboard      │
                          └───────┬───────────────────────┬──────────┘
                                  │ token bucket (Lua)     │ enqueue (idempotent)
                                  ▼                        ▼
                          ┌───────────────┐        ┌──────────────────┐
                          │     Redis     │◀──────▶│  Worker(s)        │
                          │  buckets      │ BLMOVE │  reliable pop     │
                          │  job_queue    │        │  process + retry  │
                          │  :processing  │        │  backoff (zset)   │
                          │  :retry (zset)│        │  dead-letter      │
                          │  dead_letter  │        └────────┬─────────┘
                          └───────────────┘                 │ persist outcome
                                                            ▼
                                                   ┌──────────────────┐
                                                   │   PostgreSQL      │
                                                   │   job history     │
                                                   └──────────────────┘
```

## Tech stack

| Layer | Choice |
|---|---|
| API | FastAPI + Uvicorn |
| Queue / rate-limit store | Redis (lists, hashes, sorted sets, Lua) |
| Durable job history | PostgreSQL + SQLAlchemy |
| Load testing | Locust |
| Tests | pytest (incl. a concurrency test) |
| Packaging | Docker + docker-compose |

## Quickstart

```bash
# 1. bring up everything (api + worker + redis + postgres)
docker compose up --build

# 2. open the dashboard
open http://localhost:8000/dashboard      # or just visit it in a browser

# 3. hit the API
curl http://localhost:8000/check/rate_limit/user_1      # {"message":"Request allowed"}
```

Run it **without** Docker (for development):

```bash
python -m venv venv && source venv/Scripts/activate     # (Windows Git Bash)
pip install -r requirements-dev.txt
docker compose up -d redis postgres                     # just the infra
uvicorn app.main:app --reload                           # terminal 1
python worker.py                                         # terminal 2
```

## API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/check/rate_limit/{user_id}` | Token-bucket check; `429` when the limit is exceeded |
| POST | `/api/submit_custom_job` | Enqueue a job `{task_type, payload}` (idempotent) |
| POST | `/api/submit_test_job` | Enqueue a random demo job |
| GET | `/api/metrics` | Queue/DLQ sizes, job counts, **p50/p95/p99 latency** |
| GET | `/api/jobs` | Last 10 job records |
| GET | `/dashboard` | Real-time monitoring UI |

## Load testing

```bash
locust -f locustfile.py --host http://localhost:8000
# open http://localhost:8089, set users/spawn-rate, and watch the RPS
```

> _Result on my machine (add yours):_ sustained **~____ req/s** across 100 users with the
> atomic limiter correctly capping each user — 0 over-limit requests.

## Performance (latency)

Latencies are measured server-side and exposed live at `/api/metrics` (and as cards on the
dashboard) — p50/p95/p99 over a rolling window, computed from samples in Redis.

| Metric | p50 | p95 | p99 |
|---|---|---|---|
| Rate-limit decision (Redis Lua round-trip) | ~1.0 ms | ~2.5 ms | ~2.7 ms |
| Job processing (worker execution) | ~0.3 ms | ~0.4 ms | ~0.4 ms |
| Job end-to-end (enqueue → completion) | ~4.3 ms | ~4.8 ms | ~4.8 ms |

_Measured locally against Dockerised Redis/Postgres; reproduce via `/api/metrics` after driving load._

## Design decisions & trade-offs

- **Atomic rate limiting via a Redis Lua script.** A naive limiter reads the token count,
  computes in the app, then writes back — two concurrent requests can both read the same
  count and both be allowed, exceeding the limit. The check-and-decrement runs as one Lua
  script instead, which Redis executes atomically. `tests/test_rate_limiter_concurrency.py`
  fires 200 simultaneous requests and asserts the limit is never exceeded.
- **At-least-once, not exactly-once.** The worker takes jobs with `BLMOVE` into a
  `processing` list and only removes them after handling. If it crashes mid-job, the job is
  recovered on restart. Exactly-once is far more expensive; at-least-once + idempotent
  handlers is the pragmatic industry default.
- **Backoff via a sorted set.** Failed jobs are scheduled into a `retry` sorted set (score =
  ready-at time) instead of being re-queued instantly, so a failing dependency isn't hammered.
- **Dead-letter queue.** After `MAX_RETRIES` a job is parked in `dead_letter_queue` for
  inspection rather than dropped or retried forever.
- **Token bucket over fixed-window** because it allows short bursts while keeping the average
  rate bounded, and avoids the boundary-spike problem of fixed windows.

## Testing

```bash
pytest -v
# tests/test_rate_limiter.py               happy-path via the API
# tests/test_rate_limiter_concurrency.py   proves the limiter is atomic under load
```
CI runs the suite against real Redis + Postgres on every push (`.github/workflows/ci.yml`).

## Project structure

```
app/
  main.py                 FastAPI app + endpoints + dashboard
  rate_limiter/limiter.py atomic token-bucket (Redis Lua)
  job_queue/
    producer.py           enqueue + idempotency check
    job_model.py          Job pydantic model
    idempotency.py        atomic SET NX EX dedup
  db/                     SQLAlchemy engine + JobRecord model
  redis_client.py         shared Redis client (env-configured)
worker.py                 reliable-queue consumer (backoff, DLQ, recovery)
locustfile.py             load test
docker-compose.yml        api + worker + redis + postgres
```

## Roadmap
- Sliding-window log limiter as a second strategy (pluggable)
- Prometheus `/metrics` + Grafana
- Multiple worker replicas with fair scheduling
