CREATE TABLE jobs (
    id          BIGSERIAL PRIMARY KEY,
    queue       TEXT NOT NULL DEFAULT 'default',
    payload     JSONB NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
