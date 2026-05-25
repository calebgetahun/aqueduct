# aqueduct

A Postgres-backed distributed task queue with Go workers and polyglot operations via a Postgres function API.

**Status:** v0.5 (in development). Stuck job reaping with configurable visibility timeout implemented. Working toward v0.6 — partial indexes, stress test, EXPLAIN analysis.

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

## Run (v0.1)

```bash
docker run --name pg-aqueduct -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=aqueduct \
  -p 5434:5432 -v pg-aqueduct-data:/var/lib/postgresql/data -d postgres:17

docker exec -i pg-aqueduct psql -U postgres -d aqueduct < sql/001_initial_schema.sql

AQUEDUCT_DATABASE_URL="postgres://postgres:postgres@localhost:5434/aqueduct" go run .
```

## Milestone plan

- **v0.1** — single worker, polling, no concurrency
- **v0.2** — multiple workers, observe race
- **v0.3** — fix race with FOR UPDATE SKIP LOCKED
- **v0.4** — retries with exponential backoff + jitter
- **v0.5** — stuck job reaping via visibility timeout
- **v0.6** — partial indexes, stress test, EXPLAIN before/after
- **v0.7** — extract operations into PL/pgSQL functions
- **v0.8** — LISTEN/NOTIFY for low-latency dispatch
- **v0.9** — Python client demonstrating polyglot operations
- **v0.10** — graceful shutdown
- **v1.0** — observability views, benchmarks, ship
