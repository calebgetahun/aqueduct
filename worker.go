package main

import (
	"context"
	"log"
	"math"
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
				//success
				if err := store.MarkCompleted(ctx, job.ID); err != nil {
					log.Printf("markComplete job %d: %v", job.ID, err)
				} else {
					log.Printf("completed job: %d", job.ID)
				}
			} else {
				//failure
				window := math.Min(30, math.Pow(2, float64(job.Attempts)))
				jitter := time.Duration(rand.Float64() * float64(window) * float64(time.Second))
				runAt := time.Now().Add(jitter)

				if err := store.MarkFailed(ctx, job.ID, runAt); err != nil {
					log.Printf("markFailed job %d: %v", job.ID, err)
				} else {
					log.Printf("failed job %d, attempts %d, next run_at %s", job.ID, job.Attempts, runAt)
				}
			}
		
		}
	}
}