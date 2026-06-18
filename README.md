# aqueduct

A Postgres-backed distributed task queue with Go workers and polyglot operations via a Postgres function API.

**Status:** v0.8. LISTEN/NOTIFY implemented; workers wake instantly on enqueue with a 30s fallback poll for reliability. Working toward v0.9 — Python client demonstrating polyglot operations.

## Architecture

Workers are Go-only and run user-registered handlers concurrently via goroutines. Clients in any language interact with the queue through a documented Postgres function API (planned for v0.7+). The schema is private; the function API is public.

## Differentiators

|                                              | aqueduct | River       | PGQueuer    | Celery         |
| -------------------------------------------- | -------- | ----------- | ----------- | -------------- |
| Worker runtime                               | Go       | Go          | Python      | Python         |
| Polyglot operations (cancel, retry, inspect) | Planned  | Insert only | Python only | Python only    |
| Broker                                       | Postgres | Postgres    | Postgres    | Redis/RabbitMQ |
| Transactional enqueue                        | ✓        | ✓           | ✓           | ✗              |
| Single-binary deploy                         | ✓        | ✓           | ✗           | ✗              |

## Non-goals

- Multi-step workflow orchestration (use Temporal)
- Throughput beyond ~5-10k jobs/sec on single Postgres (use Kafka/NATS)
- Drop-in Celery replacement for all-Python shops (use PGQueuer)

## Run

```bash
docker run --name pg-aqueduct -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=aqueduct \
  -p 5434:5432 -v pg-aqueduct-data:/var/lib/postgresql/data -d postgres:17

docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/001_initial_schema.sql
docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/002_add_retries.sql
docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/003_add_locked_at.sql
docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/004_add_indexes.sql
docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/005_add_lock_token.sql
docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/006_add_functions.sql

AQUEDUCT_DATABASE_URL="postgres://postgres:postgres@localhost:5434/aqueduct" go run .
```

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `AQUEDUCT_DATABASE_URL` | required | Postgres connection string |
| `AQUEDUCT_NUM_WORKERS` | `1` | Number of concurrent workers |
| `AQUEDUCT_VISIBILITY_TIMEOUT` | `30` | Seconds before a running job is considered stuck |
| `AQUEDUCT_REAPER_INTERVAL` | `60` | Seconds between stuck job reaper runs |

## Performance

Tested against a table of 1,000,000 jobs using `EXPLAIN ANALYZE` on the `AcquireNext` query.

| | Before index | After partial index |
| --- | --- | --- |
| Scan type | Sequential scan (1,000,011 rows) | Index scan |
| Execution time | 69.750ms | 0.095ms |
| Speedup | | ~735x |

The partial index `jobs_pending_queue_run_at` indexes only `pending` rows on `(queue, run_at)`, eliminating the sequential scan entirely. Completed and dead jobs are excluded from the index, keeping it small as the table grows.

A second partial index `jobs_running_locked_at` on `(locked_at)` where `status = 'running'` optimizes the stuck job reaper query, which scans for jobs that have been running past the visibility timeout.

## Milestone plan

- **v0.1** — single worker, polling, no concurrency
- **v0.2** — multiple workers, observe race
- **v0.3** — fix race with FOR UPDATE SKIP LOCKED
- **v0.4** — retries with exponential backoff + jitter
- **v0.5** — stuck job reaping via visibility timeout
- **v0.6** — partial indexes, stress test, EXPLAIN before/after, fencing tokens, idempotency checks
- **v0.7** — extract operations into PL/pgSQL functions, retry backoff on DB clock
- **v0.8** — LISTEN/NOTIFY for low-latency dispatch, 30s fallback poll
- **v0.9** — Python client demonstrating polyglot operations
- **v0.10** — graceful shutdown
- **v1.0** — observability views, benchmarks, ship
