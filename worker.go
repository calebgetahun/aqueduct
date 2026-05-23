package main

import (
	"context"
	"log"
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
			if err := store.MarkCompleted(ctx, job.ID); err != nil {
				log.Printf("markComplete job %d: %v", job.ID, err)
			} else {
				log.Printf("marked job complete: %d", job.ID)
			}
		
			
		}
	}
}