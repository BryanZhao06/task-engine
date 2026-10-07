package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BryanZhao06/task-engine/internal/db"
	"github.com/BryanZhao06/task-engine/internal/queue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	pool      *pgxpool.Pool
	rdb       *redis.Client
	queueName string
	dlqName   string
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

	w := &Worker{
		pool:      pool,
		rdb:       rdb,
		queueName: "tasks:default",
		dlqName:   "tasks:dlq",
	}

	// 3. Graceful Shutdown Management
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	numWorkers := 3

	log.Printf("Starting worker engine with %d concurrent goroutines...\n", numWorkers)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w.runWorkerLoop(ctx, workerID)
		}(i)
	}

	<-ctx.Done()
	log.Println("Shutdown signal received. Finishing in-flight tasks...")
	wg.Wait()
	log.Println("Worker engine stopped cleanly.")
}

func (w *Worker) runWorkerLoop(ctx context.Context, workerID int) {
	log.Printf("[Worker %d] ready and listening for jobs.\n", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker %d] stopping loop.\n", workerID)
			return
		default:
			taskIDStr, err := queue.DequeueTask(ctx, w.rdb, w.queueName, 2*time.Second)
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Worker %d] error dequeueing: %v\n", workerID, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if taskIDStr == "" {
				continue
			}

			w.processTask(ctx, workerID, taskIDStr)
		}
	}
}

func (w *Worker) processTask(ctx context.Context, workerID int, taskIDStr string) {
	taskUUID, err := uuid.Parse(taskIDStr)
	if err != nil {
		log.Printf("[Worker %d] invalid task ID: %s\n", workerID, taskIDStr)
		return
	}

	// Step A: Atomically claim task, increment attempts, and fetch metadata
	claimQuery := `
		UPDATE tasks
		SET status = 'running',
		    attempts = attempts + 1,
		    updated_at = NOW()
		WHERE id = $1 AND (status = 'pending' OR status = 'running')
		RETURNING task_type, attempts, max_retries;
	`
	var taskType string
	var attempts, maxRetries int

	err = w.pool.QueryRow(ctx, claimQuery, taskUUID).Scan(&taskType, &attempts, &maxRetries)
	if err != nil {
		log.Printf("[Worker %d] failed to claim task %s: %v\n", workerID, taskIDStr, err)
		return
	}

	log.Printf("[Worker %d] Processing task %s (type: %s, attempt %d/%d)...\n",
		workerID, taskIDStr, taskType, attempts, maxRetries)

	// Step B: Execute workload and check for errors
	taskErr := executeTaskLogic(taskType)

	if taskErr != nil {
		w.handleFailure(ctx, workerID, taskUUID, taskIDStr, taskErr.Error(), attempts, maxRetries)
		return
	}

	// Step C: Mark completed if execution succeeded
	completeQuery := `
		UPDATE tasks
		SET status = 'completed',
		    error_message = NULL,
		    updated_at = NOW()
		WHERE id = $1;
	`
	_, err = w.pool.Exec(ctx, completeQuery, taskUUID)
	if err != nil {
		log.Printf("[Worker %d] failed to mark task %s completed: %v\n", workerID, taskIDStr, err)
		return
	}

	log.Printf("[Worker %d] Completed task %s successfully!\n", workerID, taskIDStr)
}

// executeTaskLogic simulates work, triggering an intentional error for test task types.
func executeTaskLogic(taskType string) error {
	time.Sleep(1 * time.Second) // simulate processing latency

	if taskType == "failing_task" {
		return errors.New("upstream service connection timeout (simulated)")
	}

	return nil
}

// handleFailure executes exponential backoff or dead-letter routing.
func (w *Worker) handleFailure(ctx context.Context, workerID int, taskUUID uuid.UUID, taskIDStr string, errMsg string, attempts int, maxRetries int) {
	log.Printf("[Worker %d] Task %s failed: %s\n", workerID, taskIDStr, errMsg)

	if attempts < maxRetries {
		// Calculate exponential backoff: 2^(attempts-1) seconds (e.g. attempt 1 -> 1s, attempt 2 -> 2s, attempt 3 -> 4s)
		backoffDuration := time.Duration(math.Pow(2, float64(attempts-1))) * time.Second
		log.Printf("[Worker %d] Re-enqueuing task %s after %v backoff...\n", workerID, taskIDStr, backoffDuration)

		// Update database back to 'pending' with the latest error
		retryQuery := `
			UPDATE tasks
			SET status = 'pending',
			    error_message = $2,
			    updated_at = NOW()
			WHERE id = $1;
		`
		_, _ = w.pool.Exec(ctx, retryQuery, taskUUID, fmt.Sprintf("Attempt %d failed: %s", attempts, errMsg))

		// Wait out the backoff period and push back to Redis
		go func() {
			time.Sleep(backoffDuration)
			_ = queue.EnqueueTask(context.Background(), w.rdb, w.queueName, taskIDStr)
		}()
		return
	}

	// Max retries exceeded: Route to Dead-Letter Queue (DLQ)
	log.Printf("[Worker %d] Task %s exceeded max retries (%d). Routing to DLQ.\n", workerID, taskIDStr, maxRetries)

	dlqQuery := `
		UPDATE tasks
		SET status = 'failed',
		    error_message = $2,
		    updated_at = NOW()
		WHERE id = $1;
	`
	_, _ = w.pool.Exec(ctx, dlqQuery, taskUUID, fmt.Sprintf("Exceeded max retries (%d): %s", maxRetries, errMsg))

	// Push task ID to Dead-Letter Queue in Redis
	_ = queue.EnqueueTask(ctx, w.rdb, w.dlqName, taskIDStr)
}