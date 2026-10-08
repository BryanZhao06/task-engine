# Distributed Task Queue & Execution Engine

A fault-tolerant, asynchronous background task processing engine built in Go, using Redis for high-throughput FIFO queuing and PostgreSQL for transactional state tracking.

## Architecture Overview
```mermaid
flowchart TD
    Client["Client (HTTP)"] -->|"POST /tasks"| API["Producer API Gateway (Go)"]
    API -->|"(1) Save State"| Postgres[("PostgreSQL <br/> (State Machine)")]
    API -->|"(2) Push Task ID"| Redis[("Redis <br/> (FIFO Queue)")]
    Redis -->|"(3) Blocking Pop (BRPOP)"| Worker["Concurrent Worker Pool (Go)"]
    Worker -->|"(4) Update Status & Retries"| Postgres
    Worker -.->|"Max Retries Exceeded"| DLQ[("Dead-Letter Queue <br/> (Redis tasks:dlq)")]
```

### Core Components
* **Producer API Gateway (`cmd/api`)**: Accepts task requests via REST endpoints, persists task metadata in PostgreSQL with an initial `pending` state, and pushes the task ID to Redis with sub-10ms response latency.
* **Message Broker (Redis)**: Acts as an in-memory FIFO queue buffer mediating job dispatch.
* **Concurrent Worker Pool (`cmd/worker`)**: A fleet of Goroutines consuming from Redis using blocking operations (`BRPOP`), executing workloads asynchronously, and safely transitioning state.
* **Relational State Machine (PostgreSQL)**: Tracks execution lifecycle (`pending` -> `running` -> `completed` / `failed`) with indexed query performance.
* **Fault Tolerance & DLQ**: Implements automated exponential backoff ($2^{n-1}$ delay) across retries and routes permanently failing jobs to a Dead-Letter Queue (`tasks:dlq`).
* **Graceful Termination**: Handles OS interruption signals (`SIGINT`, `SIGTERM`) via `sync.WaitGroup` to allow in-flight tasks to finish without state corruption.

## Tech Stack
* **Language:** Go (1.23+)
* **Data Store:** PostgreSQL 16
* **Message Broker:** Redis 7
* **Containerization:** Docker, Docker Compose (Multi-stage build)

## Getting Started

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### Running the Cluster
```bash
# Clone the repository
git clone https://github.com/BryanZhao06/task-engine.git
cd task-engine

# Start all services (PostgreSQL, Redis, API Gateway, Worker Engine)
docker compose up --build -d
```

> **Troubleshooting / Clean Rebuild:**
> 
> If PostgreSQL volumes were previously created before initialization scripts were mounted, reset your local volume to execute the fresh migration:
```bash
docker compose down -v
docker compose up --build -d
```

## API Usage & Demo

### 1. Dispatch an Asynchronous Task
Submit a job request to the producer API. The engine immediately enqueues the task, records initial state in PostgreSQL, and returns an HTTP `202 Accepted` response with the assigned task UUID.

```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "generate_report",
    "payload": {"report_id": 1042}
  }'
```

#### PowerShell Alternative
```powershell
$body = @{ task_type = "generate_report"; payload = @{ report_id = 1042 } } | ConvertTo-Json
$res = Invoke-RestMethod -Uri "http://localhost:8080/tasks" -Method Post -Body $body -ContentType "application/json"
$res
```
#### Expected Response (`202 Accepted`)
```json
{
  "id": "e3b0c442-98fc-1c14-9afb-4c8996fb9242",
  "task_type": "generate_report",
  "payload": {
    "report_id": 1042
  },
  "status": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2026-10-07T16:20:00Z",
  "updated_at": "2026-10-07T16:20:00Z"
}
```

### 2. Poll Task Execution Status
Query PostgreSQL via the API to inspect the lifecycle transition (pending -> running -> completed / failed).

```bash
TASK_ID=$(curl -s -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "generate_report", "payload": {"report_id": 1042}}' | grep -o '"id":"[^"]*' | cut -d'"' -f4)

curl http://localhost:8080/tasks/$TASK_ID
```

#### PowerShell Alternative
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/tasks/$($res.id)" -Method Get
```

#### Expected Response (`200 OK`)
```json
{
  "id": "e3b0c442-98fc-1c14-9afb-4c8996fb9242",
  "task_type": "generate_report",
  "payload": {
    "report_id": 1042
  },
  "status": "completed",
  "attempts": 1,
  "max_retries": 3,
  "created_at": "2026-10-07T16:20:00Z",
  "updated_at": "2026-10-07T16:20:02Z"
}
```

### 3. Testing Fault Tolerance & DLQ
Dispatch an intentionally failing task to observe exponential backoff ($2^{n-1}$ seconds) and DLQ routing:
```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "failing_task", "payload": {"test": true}}'
```

#### PowerShell Alternative
```powershell
$failBody = @{ task_type = "failing_task"; payload = @{ test = $true } } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/tasks" -Method Post -Body $failBody -ContentType "application/json"
```

* **Attempt 1:** Fails immediately $\rightarrow$ Re-queued after a 1-second backoff.
* **Attempt 2:** Fails $\rightarrow$ Re-queued after a 2-second backoff.
* **Attempt 3:** Exceeds `max_retries` $\rightarrow$ Updated to `status: failed` in PostgreSQL and routed to the Dead Letter Queue `tasks:dlq`.

After ~7 seconds, verify the poisoned task ID landed in the Redis Dead-Letter Queue:
```bash
docker exec -it engine_redis redis-cli LRANGE tasks:dlq 0 -1
```

## Database Schema & State Machine

Workflows transition strictly through an atomic state machine tracked in PostgreSQL:

```
[ pending ] ──(Worker Claims)──> [ running ] ──(Success)──> [ completed ]
                                      │
                                (Job Failure)
                                      │
                     ┌────────────────┴────────────────┐
                     ▼                                 ▼
             (attempts < max)                  (attempts >= max)
                     │                                 │
                     ▼                                 ▼
              [ pending ] (Retry)                [ failed ] ──> (DLQ)
```

### Table Schema (`tasks`)
* `id` (`UUID`): Primary key, generated via `gen_random_uuid()`.
* `task_type` (`VARCHAR(64)`): Identifier for the task worker handler.
* `payload` (`JSONB`): Arbitrary task metadata and parameters.
* `status` (`task_status ENUM`): State restricted to `'pending'`, `'running'`, `'completed'`, or `'failed'`.
* `attempts` / `max_retries` (`INT`): Counters tracking execution retries.
* `error_message` (`TEXT`): Diagnostic output for failures.
* **Partial Index**: Indexed on `(status, created_at) WHERE status = 'pending'` for efficient worker polling without table scans.

## Engineering Highlights & Design Decisions

* **Decoupled Architecture:** Using Redis solely as a lightweight FIFO transit broker preserves in-memory throughput, while PostgreSQL handles durable persistence, queryability, and auditability.
* **Atomic State Claiming:** Workers claim tasks via `UPDATE tasks SET status = 'running', attempts = attempts + 1 WHERE id = $1 AND (status = 'pending' OR status = 'running') RETURNING ...`, eliminating race conditions between concurrent goroutines.
* **Non-Blocking Ingestion:** The HTTP API gateway offloads heavy workloads immediately, returning `202 Accepted` in sub-10ms.
* **Graceful Worker Shutdown:** Intercepts `os.Interrupt` and `SIGTERM` signals via Go context cancellation and coordinates in-flight task completion with `sync.WaitGroup`, preventing data corruption or orphaned running states on deployment restarts.
* **Multi-Stage Container Builds:** Statically compiles lightweight Linux binaries on Alpine base images, reducing final image footprint to under 20MB.
* **Dual-Write Consistency & Orphan Sweeper:** Solves producer dual-write anomalies by triggering an immediate compensating PostgreSQL rollback if Redis enqueuing fails. Includes a background sweeper goroutine that periodically queries and re-enqueues orphaned pending tasks if an API node experiences a hard crash mid-dispatch.

## Project Structure

```text
├── cmd/
│   ├── api/             # HTTP API Gateway (Task Producer)
│   └── worker/          # Background Consumer Fleet & Goroutine Pool
├── internal/
│   ├── db/              # PostgreSQL connection pool initialization (pgx/v5)
│   └── queue/           # Redis queue operations & DLQ dispatcher (go-redis)
├── migrations/
│   └── 001_create_tasks.sql # Database schema, enums, and partial indexes
├── docker-compose.yml   # Multi-container orchestration (App, Worker, Postgres, Redis)
├── Dockerfile           # Optimized multi-stage Go build
├── go.sum
└── go.mod
```

## Configuration

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `PG_CONN` | `postgres://user:password@localhost:5432/taskengine?sslmode=disable` | PostgreSQL connection pool DSN |
| `REDIS_ADDR` | `localhost:6379` | Host and port for Redis broker & DLQ |
| `PORT` | `8080` | HTTP port for the Producer API Gateway |
| `WORKER_CONCURRENCY` | `5` | Number of parallel worker Goroutines spawned |
