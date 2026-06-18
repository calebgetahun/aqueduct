package main

import (
	"context"
	"log"
	"math/rand/v2"
	"time"
)

func RunWorker(ctx context.Context, store *Store, queue string) {
	ticker := time.NewTicker(time.Millisecond * 500)

	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			job, err := store.AcquireNext(ctx, queue)
			if err != nil {
				log.Printf("acquireNext: %v", err)
				continue
			}

			if job == nil {
				continue
			}
			
			log.Printf("acquired job %d from queue %s", job.ID, job.Queue)

			if rand.IntN(2) == 0 {
				// success
				if err := store.MarkCompleted(ctx, job.ID, job.LockToken); err != nil {
					log.Printf("markComplete job %d: %v", job.ID, err)
				} else {
					log.Printf("completed job: %d", job.ID)
				}
			} else {
				if err := store.MarkFailed(ctx, job.ID, job.LockToken); err != nil {
					log.Printf("markFailed job %d: %v", job.ID, err)
				} else {
					log.Printf("failed job %d, attempts %d", job.ID, job.Attempts)
				}
			}
		
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