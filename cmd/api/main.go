package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
	"os"

	"github.com/BryanZhao06/task-engine/internal/db"
	"github.com/BryanZhao06/task-engine/internal/queue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// TaskServer holds shared database and queue dependencies for HTTP handlers.
type TaskServer struct {
	pool      *pgxpool.Pool
	rdb       *redis.Client
	queueName string
}

// CreateTaskRequest defines the incoming JSON structure.
type CreateTaskRequest struct {
	TaskType string          `json:"task_type"`
	Payload  json.RawMessage `json:"payload"`
}

// TaskResponse represents a task returned to the client.
type TaskResponse struct {
	ID           string          `json:"id"`
	TaskType     string          `json:"task_type"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Attempts     int             `json:"attempts"`
	MaxRetries   int             `json:"max_retries"`
	ErrorMessage *string         `json:"error_message,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

func main() {
	// 1. Initialize PostgreSQL Connection Pool
	pgConn := os.Getenv("PG_CONN")
	if pgConn == "" {
		pgConn = "postgres://user:password@localhost:5432/taskengine?sslmode=disable"
	}
	pool, err := db.ConnectPostgres(pgConn)
	if err != nil {
		log.Fatalf("PostgreSQL connection failed: %v", err)
	}
	defer pool.Close()

	// 2. Initialize Redis Client
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb, err := queue.ConnectRedis(redisAddr, "", 0)
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	defer rdb.Close()

	server := &TaskServer{
		pool:      pool,
		rdb:       rdb,
		queueName: "tasks:default",
	}

	// 3. Define HTTP Routes using Go's standard mux (Go 1.22+ pattern matching)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", server.handleCreateTask)
	mux.HandleFunc("GET /tasks/{id}", server.handleGetTask)

	port := ":8080"
	log.Printf("Task Engine API server listening on http://localhost%s\n", port)
	if err := http.ListenAndServe(port, mux); err != nil {
		log.Fatalf("Server terminated: %v", err)
	}
}

// handleCreateTask accepts tasks, persists to Postgres, and enqueues to Redis.
func (s *TaskServer) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if req.TaskType == "" {
		http.Error(w, `{"error":"task_type is required"}`, http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage("{}")
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Step A: Insert into PostgreSQL with 'pending' status
	query := `
		INSERT INTO tasks (task_type, payload, status)
		VALUES ($1, $2, 'pending')
		RETURNING id, created_at, updated_at;
	`
	var taskID uuid.UUID
	var createdAt, updatedAt time.Time

	err := s.pool.QueryRow(ctx, query, req.TaskType, req.Payload).Scan(&taskID, &createdAt, &updatedAt)
	if err != nil {
		log.Printf("Failed to insert task to Postgres: %v", err)
		http.Error(w, `{"error":"failed to create task"}`, http.StatusInternalServerError)
		return
	}

	// Step B: Push task ID into Redis FIFO list
	if err := queue.EnqueueTask(ctx, s.rdb, s.queueName, taskID.String()); err != nil {
		log.Printf("Failed to enqueue task %s to Redis: %v", taskID, err)
		http.Error(w, `{"error":"failed to dispatch task"}`, http.StatusInternalServerError)
		return
	}

	// Step C: Return 202 Accepted with task metadata
	res := TaskResponse{
		ID:         taskID.String(),
		TaskType:   req.TaskType,
		Payload:    req.Payload,
		Status:     "pending",
		Attempts:   0,
		MaxRetries: 3,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(res)
}

// handleGetTask queries PostgreSQL to check task state by UUID.
func (s *TaskServer) handleGetTask(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	taskUUID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, `{"error":"invalid uuid format"}`, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	query := `
		SELECT id, task_type, payload, status, attempts, max_retries, error_message, created_at, updated_at
		FROM tasks
		WHERE id = $1;
	`

	var t TaskResponse
	var rawID uuid.UUID

	err = s.pool.QueryRow(ctx, query, taskUUID).Scan(
		&rawID,
		&t.TaskType,
		&t.Payload,
		&t.Status,
		&t.Attempts,
		&t.MaxRetries,
		&t.ErrorMessage,
		&t.CreatedAt,
		&t.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, `{"error":"task not found"}`, http.StatusNotFound)
		return
	} else if err != nil {
		log.Printf("Database lookup failed: %v", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	t.ID = rawID.String()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(t)
}