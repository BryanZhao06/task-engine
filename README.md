# Distributed Task Queue & Execution Engine

A fault-tolerant, asynchronous background task processing engine built in Go, using Redis for high-throughput FIFO queuing and PostgreSQL for transactional state tracking.

## Architecture Overview

                  +-------------------+
                  |   Client (HTTP)   |
                  +---------+---------+
                            |
                            v
                  +-------------------+
                  |  Producer API     |
                  |  Gateway (Go)     |
                  +----+---------+----+
                       |         |
  (1) Save State       |         | (2) Push Task ID
                       v         v
          +---------------+   +---------------+
          |  PostgreSQL   |   |     Redis     |
          | (State Store) |   | (Queue / DLQ) |
          +---------------+   +-------+-------+
                                      |
                                      | (3) Blocking Pop (BRPOP)
                                      v
                            +-------------------+
                            | Concurrent Worker |
                            |   Fleet (Go)      |
                            +-------------------+

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
git clone [https://github.com/BryanZhao06/task-engine.git](https://github.com/BryanZhao06/task-engine.git)
cd task-engine

# Start all services (PostgreSQL, Redis, API Gateway, Worker Engine)
docker compose up --build -d

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

## Expected Response (202 Accepted)
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

### 2. Poll Task Execution Status
Query PostgreSQL via the API to inspect the lifecycle transition (pending -> running -> completed / failed).

```bash
curl http://localhost:8080/tasks/e3b0c442-98fc-1c14-9afb-4c8996fb9242

## Expected Response (200 OK)
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