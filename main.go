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
	dsn := os.Getenv("AQUEDUCT_DATABASE_URL")

	if dsn == "" {
		log.Fatal("AQUEDUCT_DATABASE_URL is required")
	}

	bgCtx := context.Background()
	pool, err := pgxpool.New(bgCtx, dsn)
	if err != nil {
		log.Fatalf("connect to db: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(bgCtx); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	log.Println("connected to DB")

	mux := http.NewServeMux()
	store := &Store{db: pool}
	server := &server{store: store}
	
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
	go RunListener(ctx, pool, notify)

	for i := range numWorkers {
		log.Printf("starting worker %d on queue: default", i)
		go RunWorker(ctx, store, "default", notify)
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
	go RunReaper(ctx, store, visibilityTimeout, reaperInterval)

	log.Fatal(http.ListenAndServe(":8080", mux))

}