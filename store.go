package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)


type Job struct {
	ID        int64           `json:"id"`
	Queue     string          `json:"queue"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	RunAt     time.Time       `json:"run_at"`
	CreatedAt time.Time       `json:"created_at"`
}

type Store struct {
	db *pgxpool.Pool
}

// Enqueue inserts a new pending job.
func (s *Store) Enqueue(ctx context.Context, queue string, payload []byte) (*Job, error) {
	var job Job

	err := s.db.QueryRow(ctx, 
		`INSERT INTO jobs (queue, payload) 
		VALUES ($1, $2) 
		RETURNING id, queue, payload, status, run_at, created_at`, 
		queue, payload).Scan(&job.ID, &job.Queue, &job.Payload, &job.Status, &job.RunAt, &job.CreatedAt)
	
	if err != nil {
		return nil, err
	}

	return &job, nil
}

// AcquireNext atomically claims the next pending job whose run_at has passed.
// Returns nil, nil if nothing is available.
func (s *Store) AcquireNext(ctx context.Context, queue string) (*Job, error) {
	var job Job

	err := s.db.QueryRow(ctx,
	`UPDATE jobs 
	SET status = 'running' 
	WHERE id = (
		SELECT id
		FROM jobs
		WHERE status = 'pending' AND run_at <= now() AND queue = $1
		ORDER BY run_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	)
	RETURNING id, queue, payload, status, run_at, created_at`,
	queue).Scan(&job.ID, &job.Queue, &job.Payload, &job.Status, &job.RunAt, &job.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	
	return &job, nil
}

// MarkCompleted transitions a running job to completed.
func (s *Store) MarkCompleted(ctx context.Context, id int64) error {
	_, err := s.db.Exec(ctx,
	`UPDATE jobs
	SET status = 'completed'
	WHERE id = $1`,
	id)

	return err
}
