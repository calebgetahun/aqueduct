package main

import (
	"context"
	"log"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RunWorker(ctx context.Context, store *Store, queue string, notify <-chan string) {
	fallback := time.NewTicker(30 * time.Second)
	defer fallback.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case q := <-notify:
			process(ctx, store, q)

		case <-fallback.C:
			process(ctx, store, queue)
		}
	}
}

func process(ctx context.Context, store *Store, queue string) {
	job, err := store.AcquireNext(ctx, queue)
	if err != nil {
		log.Printf("acquireNext: %v", err)
		return
	}

	if job == nil {
		return
	}

	log.Printf("acquired job %d from queue %s", job.ID, job.Queue)

	if rand.IntN(2) == 0 {
		if err := store.MarkCompleted(ctx, job.ID, job.LockToken); err != nil {
			log.Printf("markCompleted job %d: %v", job.ID, err)
		} else {
			log.Printf("completed job %d", job.ID)
		}
	} else {
		if err := store.MarkFailed(ctx, job.ID, job.LockToken); err != nil {
			log.Printf("markFailed job %d: %v", job.ID, err)
		} else {
			log.Printf("failed job %d, attempts %d", job.ID, job.Attempts)
		}
	}
}

func RunListener(ctx context.Context, pool *pgxpool.Pool, notify chan<- string) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("listener: acquire connection: %v", err)
		return
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN aqueduct_jobs"); err != nil {
		log.Printf("listener: LISTEN: %v", err)
		return
	}

	log.Println("listener: ready")

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("listener: %v", err)
			return
		}

		select {
		case notify <- notification.Payload:
		default:
		}
	}
}

func RunReaper(ctx context.Context, store *Store, visibilityTimeout time.Duration, reaperInterval time.Duration) {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			reapedCount, err := store.ReapStuck(ctx, visibilityTimeout)
			if err != nil {
				log.Printf("reapStuck: %v", err)
				continue
			}
			if reapedCount > 0 {
				log.Printf("reaped %d stuck jobs", reapedCount)
			}
		}
	}
}
