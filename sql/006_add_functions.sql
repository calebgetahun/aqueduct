CREATE OR REPLACE FUNCTION enqueue(
    p_queue        TEXT,
    p_payload      JSONB,
    p_max_attempts INT,
    p_run_at       TIMESTAMPTZ DEFAULT NULL
) RETURNS TABLE (
    id           BIGINT,
    queue        TEXT,
    payload      JSONB,
    status       TEXT,
    max_attempts INT,
    attempts     INT,
    run_at       TIMESTAMPTZ,
    created_at   TIMESTAMPTZ
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    INSERT INTO jobs (queue, payload, max_attempts, run_at)
    VALUES (p_queue, p_payload, p_max_attempts, COALESCE(p_run_at, now()))
    RETURNING jobs.id, jobs.queue, jobs.payload, jobs.status, jobs.max_attempts, jobs.attempts, jobs.run_at, jobs.created_at;

    PERFORM pg_notify('aqueduct_jobs', p_queue);
END;
$$;

CREATE OR REPLACE FUNCTION acquire_next(
    p_queue TEXT
) RETURNS TABLE (
    id           BIGINT,
    queue        TEXT,
    payload      JSONB,
    status       TEXT,
    max_attempts INT,
    attempts     INT,
    run_at       TIMESTAMPTZ,
    created_at   TIMESTAMPTZ,
    locked_at    TIMESTAMPTZ,
    lock_token   BIGINT
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    UPDATE jobs
    SET status = 'running', locked_at = now(), lock_token = jobs.lock_token + 1
    WHERE jobs.id = (
        SELECT j.id
        FROM jobs j
        WHERE j.status = 'pending' AND j.run_at <= now() AND j.queue = p_queue
        ORDER BY j.run_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING jobs.id, jobs.queue, jobs.payload, jobs.status, jobs.max_attempts, jobs.attempts, jobs.run_at, jobs.created_at, jobs.locked_at, jobs.lock_token;
END;
$$;

CREATE OR REPLACE FUNCTION mark_completed(
    p_id         BIGINT,
    p_lock_token BIGINT
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    UPDATE jobs
    SET status = 'completed'
    WHERE id = p_id AND lock_token = p_lock_token AND status = 'running';
END;
$$;

-- Backoff is computed here using the DB clock: run_at = now() + random() * min(30, 2^attempts) seconds.
-- This keeps the retry schedule on the same clock as the run_at <= now() check in acquire_next.
CREATE OR REPLACE FUNCTION mark_failed(
    p_id         BIGINT,
    p_lock_token BIGINT
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    UPDATE jobs
    SET
        attempts = attempts + 1,
        status = CASE
            WHEN attempts + 1 >= max_attempts THEN 'dead'
            ELSE 'pending'
        END,
        run_at = CASE
            WHEN attempts + 1 >= max_attempts THEN run_at
            ELSE now() + (random() * LEAST(30, pow(2, attempts)) * interval '1 second')
        END
    WHERE id = p_id AND lock_token = p_lock_token AND status = 'running';
END;
$$;

CREATE OR REPLACE FUNCTION reap_stuck(
    p_visibility_timeout_seconds FLOAT
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
    rows_affected BIGINT;
BEGIN
    UPDATE jobs
    SET
        attempts   = attempts + 1,
        lock_token = lock_token + 1,
        status = CASE
            WHEN attempts + 1 >= max_attempts THEN 'dead'
            ELSE 'pending'
        END,
        run_at = CASE
            WHEN attempts + 1 >= max_attempts THEN run_at
            ELSE now() + (random() * pow(2, attempts + 1)) * interval '1 second'
        END
    WHERE status = 'running'
      AND locked_at <= now() - (p_visibility_timeout_seconds * interval '1 second');

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected;
END;
$$;

CREATE OR REPLACE FUNCTION inspect_job(
    p_id BIGINT
) RETURNS TABLE (
    id           BIGINT,
    queue        TEXT,
    payload      JSONB,
    status       TEXT,
    max_attempts INT,
    attempts     INT,
    run_at       TIMESTAMPTZ,
    created_at   TIMESTAMPTZ,
    locked_at    TIMESTAMPTZ
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT jobs.id, jobs.queue, jobs.payload, jobs.status, jobs.max_attempts, jobs.attempts, jobs.run_at, jobs.created_at, jobs.locked_at
    FROM jobs
    WHERE jobs.id = p_id;
END;
$$;

CREATE OR REPLACE FUNCTION cancel_job(
    p_id BIGINT
) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE
    rows_affected INT;
BEGIN
    UPDATE jobs
    SET status = 'cancelled'
    WHERE id = p_id AND status = 'pending';

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$;
