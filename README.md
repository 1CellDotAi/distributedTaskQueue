# Distributed Task Queue System

A fault-tolerant, horizontally scalable task queue built with **Go**, **Redis**, **PostgreSQL**,
**React**, and **WebSockets**.

It supports:

- **Priority scheduling** via Redis sorted sets (lower priority value = more urgent).
- **Exponential backoff retries** with full jitter and a separate `delayed:*` ZSET that a
  coordinator promotes back to the ready queue.
- **Heartbeat-based failure detection**: workers refresh a Redis TTL key every few seconds; if
  the key disappears, the coordinator reclaims their in-flight tasks (target recovery < 5s).
- **At-least-once delivery + idempotency** via an atomic Lua `ZPOPMIN` + `HSET` lease, with
  explicit `ACK` / `NACK` and a per-worker in-flight hash.
- **Dead-letter queue** for tasks that exceed `max_attempts`, with re-drive from the dashboard.
- **Zero data loss**: tasks are persisted to PostgreSQL *before* they are enqueued in Redis, and
  the coordinator reconciles orphaned `running` rows on worker death.
- **Live dashboard** (React + TanStack Query) that subscribes to a WebSocket fed by Redis
  pub/sub.

## Repository layout

```
cmd/
  api/         REST + WebSocket server
  worker/      task-executing worker (handler registry)
  scheduler/   coordinator (delayed promoter + dead-worker reclaimer)
  loadtest/    submits N tasks and reports failure rate
internal/
  config/      env-var based configuration
  store/       PostgreSQL repository (pgx)
  queue/       Redis abstraction (priority, delayed, in-flight, DLQ)
  task/        Task model, handler interface, sample handlers
  scheduler/   Worker runtime + Coordinator
  api/         HTTP handlers (chi)
  ws/          WebSocket hub fed by Redis pub/sub
migrations/    SQL schema
web/           React + Vite dashboard
scripts/       loadtest.sh, kill-worker.sh
deploy/        (placeholder for k8s/extra manifests)
```

## Architecture

```
   ┌────────┐    POST /api/tasks    ┌──────────────┐    ZADD     ┌────────┐
   │ Client ├──────────────────────▶│  API server  ├────────────▶│ Redis  │
   └────────┘                       └──────┬───────┘  publish    └───┬────┘
                                          │ persist                  │ ZPOPMIN
                                          ▼                          ▼
                                    ┌──────────────┐         ┌──────────────┐
                                    │  PostgreSQL  │◀────────│   Workers    │
                                    │  (truth)     │ updates │  N replicas  │
                                    └──────┬───────┘         └──────┬───────┘
                                          ▲                          │ heartbeat
                                          │ requeue orphans          ▼
                                   ┌──────┴───────┐         ┌──────────────┐
                                   │ Coordinator  │◀────────│ hb:{worker}  │ (TTL)
                                   └──────────────┘  scan   └──────────────┘
                                          │ pub/sub events
                                          ▼
                                    ┌──────────────┐  WebSocket  ┌──────────┐
                                    │  Redis bus   ├────────────▶│ Dashboard│
                                    └──────────────┘             └──────────┘
```

### Redis key layout

| Key                  | Type | Purpose                                              |
| -------------------- | ---- | ---------------------------------------------------- |
| `queue:{type}`       | ZSET | Ready tasks. Score = `priority*1e13 + enqueueMs`.    |
| `delayed:{type}`     | ZSET | Retry/delay; score = `available_at` epoch ms.        |
| `inflight:{worker}`  | HASH | Tasks currently leased by a worker (`task_id → env`).|
| `hb:{worker}`        | STR  | Heartbeat with TTL (default 9s, refreshed every 3s). |
| `dlq:{type}`         | LIST | Dead-letter envelopes.                               |
| `events:tasks`       | PUB/SUB | Task lifecycle events; fanned out to WebSockets.  |

## Quick start (docker compose)

```bash
# Start Postgres, Redis, API, 3 workers, scheduler, and the dashboard.
docker compose up --build

# Dashboard:  http://localhost:5173
# API:        http://localhost:8080/api
```

Submit a task:

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":{"hello":"world"},"priority":1,"max_attempts":3}'
```

## Quick start (local Go, without containers)

```bash
# 1. Start Postgres + Redis (any way you like — docker, brew, etc.).
export DATABASE_URL='host=localhost port=5432 user=postgres dbname=taskq sslmode=disable'
export PGPASSWORD='postgres'
export REDIS_URL='redis://localhost:6379/0'

# 2. Run each component in its own terminal:
go run ./cmd/api
go run ./cmd/worker
go run ./cmd/scheduler

# 3. Run the dashboard:
cd web && npm install && npm run dev   # http://localhost:5173
```

## API

| Method & path                         | Description                                       |
| ------------------------------------- | ------------------------------------------------- |
| `POST /api/tasks`                     | Submit a task `{type, payload, priority, max_attempts}` |
| `GET /api/tasks?status=&type=&limit=` | List tasks                                        |
| `GET /api/tasks/{id}`                 | Fetch one task                                    |
| `GET /api/tasks/{id}/events`          | Task audit trail                                  |
| `POST /api/tasks/{id}/cancel`         | Cancel a queued/retrying task                     |
| `GET /api/workers`                    | List workers + last heartbeat                     |
| `GET /api/dlq`                        | List dead-letter records                          |
| `POST /api/dlq/{id}/redrive`          | Re-enqueue a dead task                            |
| `GET /api/stats`                      | Aggregate stats: counts by status, queue depths   |
| `GET /healthz` / `GET /readyz`        | Liveness / readiness                              |
| `GET /ws`                             | WebSocket; streams `task.Event` JSON payloads     |

## Configuration (env vars)

| Var                          | Default                                                              |
| ---------------------------- | -------------------------------------------------------------------- |
| `DATABASE_URL`               | `host=localhost port=5432 user=postgres dbname=taskq sslmode=disable`|
| `PGPASSWORD`                 | (read by libpq if password not in `DATABASE_URL`)                    |
| `REDIS_URL`                  | `redis://localhost:6379/0`                                           |
| `HTTP_ADDR`                  | `:8080`                                                              |
| `WORKER_CONCURRENCY`         | `4`                                                                  |
| `HEARTBEAT_INTERVAL`         | `3s`                                                                 |
| `HEARTBEAT_TTL`              | `9s`                                                                 |
| `COORDINATOR_SWEEP_INTERVAL` | `1s`                                                                 |
| `RETRY_BASE_DELAY`           | `500ms`                                                              |
| `RETRY_MAX_DELAY`            | `60s`                                                                |
| `LEASE_TTL`                  | `60s`                                                                |
| `TASK_TYPES`                 | (empty = all registered handler types)                               |

## Built-in handlers

| Type    | Description                                                       |
| ------- | ----------------------------------------------------------------- |
| `echo`  | Returns the payload unchanged.                                    |
| `sleep` | `{seconds: float}` — sleeps then returns.                         |
| `flaky` | `{fail_rate: 0..1}` — fails probabilistically; exercises retries. |
| `email` | `{to, subject}` — validates and logs a "sent" event.              |

Register your own with `task.NewRegistry().Register("yourtype", yourHandler)` in `cmd/worker/main.go`.

## Validation tests (back the resume numbers)

Make sure the stack is up (`docker compose up --build`) and run:

```bash
# Submit 10,000 tasks with a 5% transient failure rate.
./scripts/loadtest.sh 10000 64 flaky
# Expected output ends with: FINAL by_status=... dead=0 failure_rate=0.0000%
```

Crash-recovery test (in a separate terminal while loadtest is running):

```bash
./scripts/kill-worker.sh
# Observe in the API logs / dashboard that the coordinator detects the dead
# worker within ~5s, requeues in-flight tasks, and they complete on other workers.
```

DLQ test (force exhaustion):

```bash
curl -X POST http://localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"flaky","payload":{"fail_rate":1.0},"priority":5,"max_attempts":2}'
# The task will retry then move to the DLQ. View / redrive in the dashboard.
```

## Running tests

```bash
go test ./...
```

Unit tests cover the Redis queue layer (priority ordering, lease/ack, delayed promotion,
in-flight reclaim, DLQ, heartbeat TTL) using [miniredis](https://github.com/alicebob/miniredis),
plus the exponential-backoff calculator and handler registry.

## License

MIT.
