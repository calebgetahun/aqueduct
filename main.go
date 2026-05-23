package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

type server struct {
	store *Store
}

type createJobRequest struct {
	Queue		string				`json:"queue"`
	Payload		json.RawMessage		`json:"payload"`
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

	job, err := s.store.Enqueue(r.Context(), req.Queue, req.Payload)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	log.Println("starting worker on queue: default")
	go RunWorker(ctx, store, "default")

	
	log.Fatal(http.ListenAndServe(":8080", mux))

}