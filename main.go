package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type server struct {
	store *Store
}

type createJobRequest struct {
	Queue			string				`json:"queue"`
	Payload			json.RawMessage		`json:"payload"`
	MaxAttempts 	*int				`json:"max_attempts"`
	RunAt			*time.Time			`json:"run_at"`
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("decode: %v", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req.Payload) == 0 {
		http.Error(w, "missing payload", http.StatusBadRequest)
		return
	}

	if req.Queue == "" {
		req.Queue = "default"
	}

	defaultAttempts := 3

	if req.MaxAttempts == nil {
		req.MaxAttempts = &defaultAttempts
	} else if *req.MaxAttempts <= 0 {
		http.Error(w, "invalid max attempts", http.StatusBadRequest)
		return
	}

	job, err := s.store.Enqueue(r.Context(), req.Queue, req.Payload, req.MaxAttempts, req.RunAt)

	if err != nil {
		log.Printf("enqueue: %v", err)
		http.Error(w, "internal DB error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Printf("encoding: %v", err)
	}

}

func (s *server) deleteJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("job_id")
	idNum, err := strconv.Atoi(idStr)

	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	err = s.store.CancelJob(r.Context(), int64(idNum))

	if errors.Is(err, ErrJobNotFound) {
		http.Error(w, "job not found or cancellable", http.StatusNotFound)
		return
	}

	if err != nil {
		log.Printf("cancelJob: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	log.Printf("cancelled job %d", idNum)

	w.WriteHeader(http.StatusNoContent)
}

func main() {
	producerDSN := os.Getenv("AQUEDUCT_PRODUCER_DATABASE_URL")
	workerDSN := os.Getenv("AQUEDUCT_WORKER_DATABASE_URL")

	if producerDSN == "" {
		log.Fatal("AQUEDUCT_PRODUCER_DATABASE_URL is required")
	}
	if workerDSN == "" {
		log.Fatal("AQUEDUCT_WORKER_DATABASE_URL is required")
	}

	bgCtx := context.Background()

	producerPool, err := pgxpool.New(bgCtx, producerDSN)
	if err != nil {
		log.Fatalf("connect to db as producer: %v", err)
	}
	defer producerPool.Close()

	workerPool, err := pgxpool.New(bgCtx, workerDSN)
	if err != nil {
		log.Fatalf("connect to db as worker: %v", err)
	}
	defer workerPool.Close()

	if err := producerPool.Ping(bgCtx); err != nil {
		log.Fatalf("ping db as producer: %v", err)
	}
	if err := workerPool.Ping(bgCtx); err != nil {
		log.Fatalf("ping db as worker: %v", err)
	}

	log.Println("connected to DB")

	mux := http.NewServeMux()
	producerStore := &Store{db: producerPool}
	workerStore := &Store{db: workerPool}
	server := &server{store: producerStore}

	mux.HandleFunc("POST /jobs", server.createJob)
	mux.HandleFunc("DELETE /jobs/{job_id}", server.deleteJob)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numWorkers := 1

	if n := os.Getenv("AQUEDUCT_NUM_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil {
			numWorkers = parsed
		}
	}

	notify := make(chan string, numWorkers)
	go RunListener(ctx, workerPool, notify)

	for i := range numWorkers {
		log.Printf("starting worker %d on queue: default", i)
		go RunWorker(ctx, workerStore, "default", notify)
	}

	reaperInterval := time.Minute
	visibilityTimeout := 30 * time.Second

	if rI := os.Getenv("AQUEDUCT_REAPER_INTERVAL"); rI != "" {
		if parsed, err := strconv.Atoi(rI); err == nil {
			reaperInterval = time.Duration(parsed) * time.Second
		}
	}

	if vT := os.Getenv("AQUEDUCT_VISIBILITY_TIMEOUT"); vT != "" {
		if parsed, err := strconv.Atoi(vT); err == nil {
			visibilityTimeout = time.Duration(parsed) * time.Second
		}
	}

	log.Println("starting reaper")
	go RunReaper(ctx, workerStore, visibilityTimeout, reaperInterval)

	log.Fatal(http.ListenAndServe(":8080", mux))

}