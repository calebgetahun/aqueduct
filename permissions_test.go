package main

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func skipIfNoRolePools(t *testing.T) {
	t.Helper()
	if producerPool == nil || workerPool == nil {
		t.Skip("AQUEDUCT_PRODUCER_DATABASE_URL / AQUEDUCT_WORKER_DATABASE_URL not set")
	}
}

// insufficient_privilege -- https://www.postgresql.org/docs/current/errcodes-appendix.html
func isPermissionDenied(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

func TestProducer_CannotSelectJobsTable(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := producerPool.Exec(context.Background(), `SELECT 1 FROM jobs LIMIT 1`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestProducer_CannotAcquireNext(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := producerPool.Exec(context.Background(), `SELECT * FROM acquire_next('default')`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestProducer_CannotMarkCompleted(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := producerPool.Exec(context.Background(), `SELECT mark_completed(1, 1)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestProducer_CannotMarkFailed(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := producerPool.Exec(context.Background(), `SELECT mark_failed(1, 1)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestProducer_CannotReapStuck(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := producerPool.Exec(context.Background(), `SELECT reap_stuck(30)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestWorker_CannotSelectJobsTable(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := workerPool.Exec(context.Background(), `SELECT 1 FROM jobs LIMIT 1`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestWorker_CannotEnqueue(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := workerPool.Exec(context.Background(), `SELECT * FROM enqueue('default', '{}'::jsonb, 3, NULL)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestWorker_CannotCancelJob(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := workerPool.Exec(context.Background(), `SELECT cancel_job(1)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestWorker_CannotUpdateJob(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := workerPool.Exec(context.Background(), `SELECT * FROM update_job(1, NULL, NULL, NULL, NULL)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestWorker_CannotDeleteJob(t *testing.T) {
	skipIfNoRolePools(t)
	_, err := workerPool.Exec(context.Background(), `SELECT delete_job(1)`)
	if !isPermissionDenied(err) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

// Sanity checks: confirm the intended access actually works, so a missing
// grant fails loudly here instead of surfacing as a confusing error elsewhere.

func TestProducer_CanEnqueueAndCancel(t *testing.T) {
	skipIfNoRolePools(t)

	var id int64
	err := producerPool.QueryRow(context.Background(),
		`SELECT id FROM enqueue('default', '{"test":true}'::jsonb, 3, NULL)`).Scan(&id)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var cancelled bool
	if err := producerPool.QueryRow(context.Background(), `SELECT cancel_job($1)`, id).Scan(&cancelled); err != nil {
		t.Fatalf("cancel_job: %v", err)
	}
	if !cancelled {
		t.Fatal("expected cancel_job to succeed on a pending job")
	}
}

func TestWorker_CanAcquireAndComplete(t *testing.T) {
	skipIfNoRolePools(t)
	clearJobs(t)
	job := enqueue(t, "default", 3)

	var id, lockToken int64
	err := workerPool.QueryRow(context.Background(),
		`SELECT id, lock_token FROM acquire_next('default')`).Scan(&id, &lockToken)
	if err != nil {
		t.Fatalf("acquire_next: %v", err)
	}
	if id != job.ID {
		t.Fatalf("expected to acquire job %d, got %d", job.ID, id)
	}

	if _, err := workerPool.Exec(context.Background(), `SELECT mark_completed($1, $2)`, id, lockToken); err != nil {
		t.Fatalf("mark_completed: %v", err)
	}
}
