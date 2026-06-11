package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var testStore *Store

func TestMain(m *testing.M) {
	dsn := os.Getenv("AQUEDUCT_DATABASE_URL")
	if dsn == "" {
		os.Exit(0)
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		panic("connect to test db: " + err.Error())
	}
	defer pool.Close()

	testStore = &Store{db: pool}
	os.Exit(m.Run())
}

func clearJobs(t *testing.T) {
	t.Helper()
	_, err := testStore.db.Exec(context.Background(), "TRUNCATE jobs RESTART IDENTITY")
	if err != nil {
		t.Fatalf("clear jobs: %v", err)
	}
}

func enqueue(t *testing.T, queue string, maxAttempts int) *Job {
	t.Helper()
	payload := json.RawMessage(`{"test": true}`)
	job, err := testStore.Enqueue(context.Background(), queue, payload, &maxAttempts, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func acquireNext(t *testing.T, queue string) *Job {
	t.Helper()
	job, err := testStore.AcquireNext(context.Background(), queue)
	if err != nil {
		t.Fatalf("acquireNext: %v", err)
	}
	return job
}

type jobRow struct {
	status    string
	attempts  int
	lockToken int64
}

func queryJob(t *testing.T, id int64) jobRow {
	t.Helper()
	var row jobRow
	err := testStore.db.QueryRow(context.Background(),
		`SELECT status, attempts, lock_token FROM jobs WHERE id = $1`, id,
	).Scan(&row.status, &row.attempts, &row.lockToken)
	if err != nil {
		t.Fatalf("queryJob %d: %v", id, err)
	}
	return row
}

// Only one worker should claim a job even if two race for it.
func TestAcquireNext_MutualExclusion(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	first := acquireNext(t, "default")
	if first == nil {
		t.Fatal("expected first acquire to succeed")
	}

	second := acquireNext(t, "default")
	if second != nil {
		t.Fatalf("expected second acquire to return nil, got job %d", second.ID)
	}
}

// AcquireNext should return nil when the queue is empty.
func TestAcquireNext_EmptyQueue(t *testing.T) {
	clearJobs(t)

	job := acquireNext(t, "default")
	if job != nil {
		t.Fatalf("expected nil from empty queue, got job %d", job.ID)
	}
}

// AcquireNext should not return jobs scheduled in the future.
func TestAcquireNext_RespectsRunAt(t *testing.T) {
	clearJobs(t)
	payload := json.RawMessage(`{"test": true}`)
	maxAttempts := 3
	future := time.Now().Add(time.Hour)
	_, err := testStore.Enqueue(context.Background(), "default", payload, &maxAttempts, &future)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job := acquireNext(t, "default")
	if job != nil {
		t.Fatalf("expected nil for future job, got job %d", job.ID)
	}
}

// AcquireNext increments lock_token on each claim.
func TestAcquireNext_IncrementsLockToken(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	job := acquireNext(t, "default")
	if job.LockToken != 1 {
		t.Fatalf("expected lock_token=1 after first acquire, got %d", job.LockToken)
	}
}

// A zombie worker holding a stale token cannot mark a job completed.
func TestFencingToken_ZombieCompleteIsNoOp(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	// Worker 1 acquires the job.
	worker1Job := acquireNext(t, "default")
	staleToken := worker1Job.LockToken // == 1

	// Reaper fires: requeues the job and bumps the token.
	_, err := testStore.db.Exec(context.Background(),
		`UPDATE jobs SET status = 'pending', lock_token = lock_token + 1 WHERE id = $1`,
		worker1Job.ID,
	)
	if err != nil {
		t.Fatalf("simulate reaper: %v", err)
	}

	// Worker 2 acquires the requeued job — token is now 3.
	worker2Job := acquireNext(t, "default")
	if worker2Job == nil {
		t.Fatal("worker 2 expected to acquire job")
	}
	if worker2Job.LockToken != 3 {
		t.Fatalf("expected lock_token=3 for worker 2, got %d", worker2Job.LockToken)
	}

	// Zombie worker 1 wakes up and tries to complete with its stale token.
	if err := testStore.MarkCompleted(context.Background(), worker1Job.ID, staleToken); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	// Job must still be running — worker 2 holds it.
	row := queryJob(t, worker1Job.ID)
	if row.status != "running" {
		t.Fatalf("expected status=running, got %q — zombie write was not blocked", row.status)
	}
}

// A zombie worker holding a stale token cannot mark a job failed (the worst case —
// it would flip a live job back to pending).
func TestFencingToken_ZombieFailIsNoOp(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	worker1Job := acquireNext(t, "default")
	staleToken := worker1Job.LockToken

	_, err := testStore.db.Exec(context.Background(),
		`UPDATE jobs SET status = 'pending', lock_token = lock_token + 1 WHERE id = $1`,
		worker1Job.ID,
	)
	if err != nil {
		t.Fatalf("simulate reaper: %v", err)
	}

	worker2Job := acquireNext(t, "default")
	if worker2Job == nil {
		t.Fatal("worker 2 expected to acquire job")
	}

	// Zombie worker 1 tries to mark the job failed.
	if err := testStore.MarkFailed(context.Background(), worker1Job.ID, staleToken, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	row := queryJob(t, worker1Job.ID)
	if row.status != "running" {
		t.Fatalf("expected status=running, got %q — zombie write was not blocked", row.status)
	}
	if row.attempts != 0 {
		t.Fatalf("expected attempts=0, got %d — zombie incremented attempts", row.attempts)
	}
}

// Calling MarkFailed twice with the same token must not double-increment attempts.
func TestMarkFailed_IdempotentOnDoubleCall(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	job := acquireNext(t, "default")
	runAt := time.Now().Add(-time.Second)

	if err := testStore.MarkFailed(context.Background(), job.ID, job.LockToken, runAt); err != nil {
		t.Fatalf("first MarkFailed: %v", err)
	}
	if err := testStore.MarkFailed(context.Background(), job.ID, job.LockToken, runAt); err != nil {
		t.Fatalf("second MarkFailed: %v", err)
	}

	row := queryJob(t, job.ID)
	if row.attempts != 1 {
		t.Fatalf("expected attempts=1 after double MarkFailed, got %d", row.attempts)
	}
}

// Calling MarkCompleted after MarkFailed with the same token must be a no-op.
func TestMarkCompleted_NoOpIfNotRunning(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	job := acquireNext(t, "default")
	if err := testStore.MarkFailed(context.Background(), job.ID, job.LockToken, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	// Job is now pending. MarkCompleted with same token must not complete it.
	if err := testStore.MarkCompleted(context.Background(), job.ID, job.LockToken); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	row := queryJob(t, job.ID)
	if row.status != "pending" {
		t.Fatalf("expected status=pending, got %q — stale MarkCompleted won", row.status)
	}
}

// A job should retry with incremented attempts on failure.
func TestMarkFailed_IncrementsAttempts(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)

	job := acquireNext(t, "default")
	if err := testStore.MarkFailed(context.Background(), job.ID, job.LockToken, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	row := queryJob(t, job.ID)
	if row.status != "pending" {
		t.Fatalf("expected status=pending, got %q", row.status)
	}
	if row.attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", row.attempts)
	}
}

// A job should go dead once attempts reaches max_attempts.
func TestMarkFailed_DeadAtMaxAttempts(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 2)

	for range 2 {
		job := acquireNext(t, "default")
		if job == nil {
			t.Fatal("expected to acquire job")
		}
		if err := testStore.MarkFailed(context.Background(), job.ID, job.LockToken, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("MarkFailed: %v", err)
		}
	}

	row := queryJob(t, 1)
	if row.status != "dead" {
		t.Fatalf("expected status=dead after max attempts, got %q", row.status)
	}
	if row.attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", row.attempts)
	}
}

// The reaper should requeue stuck running jobs and bump their lock token.
func TestReapStuck_RequeuesAndBumpsToken(t *testing.T) {
	clearJobs(t)

	// Insert a running job that has been stuck for 10 minutes.
	_, err := testStore.db.Exec(context.Background(),
		`INSERT INTO jobs (queue, payload, status, max_attempts, locked_at, lock_token)
		 VALUES ('default', '{}', 'running', 3, now() - interval '10 minutes', 1)`,
	)
	if err != nil {
		t.Fatalf("insert stuck job: %v", err)
	}

	reaped, err := testStore.ReapStuck(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatalf("ReapStuck: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("expected 1 reaped job, got %d", reaped)
	}

	row := queryJob(t, 1)
	if row.status != "pending" {
		t.Fatalf("expected status=pending after reap, got %q", row.status)
	}
	if row.attempts != 1 {
		t.Fatalf("expected attempts=1 after reap, got %d", row.attempts)
	}
	if row.lockToken != 2 {
		t.Fatalf("expected lock_token=2 after reap, got %d — zombie write window still open", row.lockToken)
	}
}

// The reaper should mark a job dead if it has exhausted its attempts.
func TestReapStuck_DeadAtMaxAttempts(t *testing.T) {
	clearJobs(t)

	_, err := testStore.db.Exec(context.Background(),
		`INSERT INTO jobs (queue, payload, status, max_attempts, attempts, locked_at)
		 VALUES ('default', '{}', 'running', 2, 1, now() - interval '10 minutes')`,
	)
	if err != nil {
		t.Fatalf("insert stuck job: %v", err)
	}

	if _, err := testStore.ReapStuck(context.Background(), 30*time.Second); err != nil {
		t.Fatalf("ReapStuck: %v", err)
	}

	row := queryJob(t, 1)
	if row.status != "dead" {
		t.Fatalf("expected status=dead, got %q", row.status)
	}
}

// The reaper should not touch jobs within the visibility timeout.
func TestReapStuck_IgnoresRecentJobs(t *testing.T) {
	clearJobs(t)

	_, err := testStore.db.Exec(context.Background(),
		`INSERT INTO jobs (queue, payload, status, max_attempts, locked_at)
		 VALUES ('default', '{}', 'running', 3, now() - interval '5 seconds')`,
	)
	if err != nil {
		t.Fatalf("insert fresh running job: %v", err)
	}

	reaped, err := testStore.ReapStuck(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatalf("ReapStuck: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("expected 0 reaped, got %d", reaped)
	}
}

// CancelJob should work on a pending job.
func TestCancelJob_CancelsPending(t *testing.T) {
	clearJobs(t)
	job := enqueue(t, "default", 3)

	if err := testStore.CancelJob(context.Background(), job.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	row := queryJob(t, job.ID)
	if row.status != "cancelled" {
		t.Fatalf("expected status=cancelled, got %q", row.status)
	}
}

// CancelJob should refuse to cancel a running job.
func TestCancelJob_RefusesRunning(t *testing.T) {
	clearJobs(t)
	enqueue(t, "default", 3)
	job := acquireNext(t, "default")

	err := testStore.CancelJob(context.Background(), job.ID)
	if err != ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound for running job, got %v", err)
	}
}

// CancelJob should return ErrJobNotFound for unknown IDs.
func TestCancelJob_NotFound(t *testing.T) {
	clearJobs(t)

	err := testStore.CancelJob(context.Background(), 99999)
	if err != ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}
