package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConnectRedis initializes and verifies a connection to the Redis server.
func ConnectRedis(addr string, password string, db int) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password, // Default is "" if no password is set
		DB:       db,       // 0 is default database
	})

	// Test the connection with a 5-second context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return rdb, nil
}

// EnqueueTask pushes a task ID into a FIFO Redis list.
func EnqueueTask(ctx context.Context, rdb *redis.Client, queueName string, taskID string) error {
	// LPUSH inserts the task ID at the head of the list
	err := rdb.LPush(ctx, queueName, taskID).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue task %s to %s: %w", taskID, queueName, err)
	}
	return nil
}

// DequeueTask blocks until a task ID is available in the queue or the context cancels.
func DequeueTask(ctx context.Context, rdb *redis.Client, queueName string, timeout time.Duration) (string, error) {
	// BRPop pops the rightmost element; if empty, it blocks for up to 'timeout'
	results, err := rdb.BRPop(ctx, timeout, queueName).Result()
	if err != nil {
		return "", err
	}
	// results[0] is the queue name, results[1] is the popped string (the task ID)
	if len(results) > 1 {
		return results[1], nil
	}
	return "", nil
}