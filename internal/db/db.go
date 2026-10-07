package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectPostgres initializes and tests a connection pool to PostgreSQL.
func ConnectPostgres(connString string) (*pgxpool.Pool, error) {
	// 1. Configure the connection pool
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database config: %w", err)
	}

	// Keep max 10 open connections, idle timeout of 30 minutes
	config.MaxConns = 10
	config.MaxConnIdleTime = 30 * time.Minute

	// 2. Establish connection with a 5-second context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// 3. Ping the database to ensure the network connection actually works
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}