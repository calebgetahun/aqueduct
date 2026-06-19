-- Passing NULL for any optional parameter leaves that column unchanged.
-- Only pending jobs may be edited; once a job is running/completed/dead/cancelled
-- it's immutable here to avoid racing a worker that's already holding the row.
CREATE OR REPLACE FUNCTION update_job(
    p_id           BIGINT,
    p_payload      JSONB DEFAULT NULL,
    p_run_at       TIMESTAMPTZ DEFAULT NULL,
    p_max_attempts INT DEFAULT NULL,
    p_queue        TEXT DEFAULT NULL
) RETURNS TABLE (
    id           BIGINT,
    queue        TEXT,
    payload      JSONB,
    status       TEXT,
    max_attempts INT,
    attempts     INT,
    run_at       TIMESTAMPTZ,
    created_at   TIMESTAMPTZ
) LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_catalog AS $$
BEGIN
    RETURN QUERY
    UPDATE jobs
    SET
        payload      = COALESCE(p_payload, jobs.payload),
        run_at       = COALESCE(p_run_at, jobs.run_at),
        max_attempts = COALESCE(p_max_attempts, jobs.max_attempts),
        queue        = COALESCE(p_queue, jobs.queue)
    WHERE jobs.id = p_id AND jobs.status = 'pending'
    RETURNING jobs.id, jobs.queue, jobs.payload, jobs.status, jobs.max_attempts, jobs.attempts, jobs.run_at, jobs.created_at;
END;
$$;

-- Hard delete. Disallowed while a job is running so we never remove a row out
-- from under a worker that's holding its lock_token mid-execution. Pending,
-- completed, dead, and cancelled jobs may all be deleted.
CREATE OR REPLACE FUNCTION delete_job(
    p_id BIGINT
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_catalog AS $$
DECLARE
    rows_affected INT;
BEGIN
    DELETE FROM jobs
    WHERE id = p_id AND status != 'running';

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$;
