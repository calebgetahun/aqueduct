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

// Enqueue inserts a new pending job.
func (s *Store) Enqueue(ctx context.Context, queue string, payload []byte, maxAttempts *int, runAt *time.Time) (*Job, error) {
	var job Job

	err := s.db.QueryRow(ctx, 
		`INSERT INTO jobs (queue, payload, max_attempts, run_at) 
		VALUES ($1, $2, $3, COALESCE($4, now())) 
		RETURNING id, queue, payload, status, max_attempts, attempts, run_at, created_at`, 
		queue, payload, *maxAttempts, runAt).Scan(&job.ID, &job.Queue, &job.Payload, &job.Status, &job.MaxAttempts, &job.Attempts, &job.RunAt, &job.CreatedAt)
	
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
	SET status = 'running', locked_at = now(), lock_token = lock_token + 1
	WHERE id = (
		SELECT id
		FROM jobs
		WHERE status = 'pending' AND run_at <= now() AND queue = $1
		ORDER BY run_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	)
	RETURNING id, queue, payload, status, max_attempts, attempts, run_at, created_at, locked_at, lock_token`,
	queue).Scan(&job.ID, &job.Queue, &job.Payload, &job.Status, &job.MaxAttempts, &job.Attempts, &job.RunAt, &job.CreatedAt, &job.LockedAt, &job.LockToken)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	
	return &job, nil
}

// MarkCompleted transitions a running job to completed.
func (s *Store) MarkCompleted(ctx context.Context, id int64, lockToken int64) error {
	_, err := s.db.Exec(ctx,
	`UPDATE jobs
	SET status = 'completed'
	WHERE id = $1 AND lock_token = $2 AND status = 'running'`,
	id, lockToken)

	return err
}

func (s *Store) MarkFailed(ctx context.Context, id int64, lockToken int64, runAt time.Time) error {
	_, err := s.db.Exec(ctx,
	`UPDATE jobs
	SET
		attempts = attempts + 1,
		status = CASE
			WHEN attempts + 1 >= max_attempts THEN 'dead'
			ELSE 'pending'
		END,
		run_at = CASE
			WHEN attempts + 1 >= max_attempts THEN run_at
			ELSE $3
		END
	WHERE id = $1 AND lock_token = $2 AND status = 'running'
	`, id, lockToken, runAt)

	return err
}

func (s *Store) ReapStuck(ctx context.Context, visibilityTimeout time.Duration) (int64, error) {
	tag, err := s.db.Exec(ctx,
	`UPDATE jobs
	SET
		attempts = attempts + 1,
		lock_token = lock_token + 1,
		status = CASE
			WHEN attempts + 1 >= max_attempts THEN 'dead'
			ELSE 'pending'
		END,
		run_at = CASE
			WHEN attempts + 1 >= max_attempts THEN run_at
			ELSE now() + (random() * pow(2, attempts + 1)) * interval '1 second'
		END
	WHERE status = 'running' AND locked_at <= now() - ($1 * interval '1 second')
	`, visibilityTimeout.Seconds())

	if err != nil {
		return 0, err
	}

	rowsReaped := tag.RowsAffected()

	return rowsReaped, nil
}

func (s *Store) CancelJob(ctx context.Context, id int64) error {
	tag, err := s.db.Exec(ctx,
	`UPDATE jobs
	SET status = 'cancelled'
	WHERE id = $1 AND status = 'pending'
	`, id)
	
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}

	return nil

}