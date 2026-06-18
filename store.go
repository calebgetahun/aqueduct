package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrJobNotFound = errors.New("job not found or not cancellable")


type Job struct {
	ID        	int64           `json:"id"`
	Queue     	string          `json:"queue"`
	Payload   	json.RawMessage `json:"payload"`
	Status    	string          `json:"status"`
	MaxAttempts	int				`json:"max_attempts"`
	Attempts	int				`json:"attempts"`
	RunAt     	time.Time       `json:"run_at"`
	CreatedAt 	time.Time       `json:"created_at"`
	LockedAt	*time.Time		`json:"locked_at"`
	LockToken	int64			`json:"lock_token"`
}

type Store struct {
	db *pgxpool.Pool
}

func (s *Store) Enqueue(ctx context.Context, queue string, payload []byte, maxAttempts *int, runAt *time.Time) (*Job, error) {
	var job Job

	err := s.db.QueryRow(ctx,
		`SELECT id, queue, payload, status, max_attempts, attempts, run_at, created_at
		 FROM enqueue($1, $2, $3, $4)`,
		queue, payload, *maxAttempts, runAt).Scan(
		&job.ID, &job.Queue, &job.Payload, &job.Status,
		&job.MaxAttempts, &job.Attempts, &job.RunAt, &job.CreatedAt)

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *Store) AcquireNext(ctx context.Context, queue string) (*Job, error) {
	var job Job

	err := s.db.QueryRow(ctx,
		`SELECT id, queue, payload, status, max_attempts, attempts, run_at, created_at, locked_at, lock_token
		 FROM acquire_next($1)`,
		queue).Scan(
		&job.ID, &job.Queue, &job.Payload, &job.Status,
		&job.MaxAttempts, &job.Attempts, &job.RunAt, &job.CreatedAt,
		&job.LockedAt, &job.LockToken)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *Store) MarkCompleted(ctx context.Context, id int64, lockToken int64) error {
	_, err := s.db.Exec(ctx, `SELECT mark_completed($1, $2)`, id, lockToken)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, id int64, lockToken int64) error {
	_, err := s.db.Exec(ctx, `SELECT mark_failed($1, $2)`, id, lockToken)
	return err
}

func (s *Store) ReapStuck(ctx context.Context, visibilityTimeout time.Duration) (int64, error) {
	var rowsReaped int64
	err := s.db.QueryRow(ctx, `SELECT reap_stuck($1)`, visibilityTimeout.Seconds()).Scan(&rowsReaped)
	if err != nil {
		return 0, err
	}
	return rowsReaped, nil
}

func (s *Store) CancelJob(ctx context.Context, id int64) error {
	var cancelled bool
	err := s.db.QueryRow(ctx, `SELECT cancel_job($1)`, id).Scan(&cancelled)
	if err != nil {
		return err
	}
	if !cancelled {
		return ErrJobNotFound
	}
	return nil
}
