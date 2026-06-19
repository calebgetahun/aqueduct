-- The jobs table is private; the function API is the contract polyglot
-- clients are meant to use. These functions need SECURITY DEFINER so a role
-- with EXECUTE-only grants (no table privileges) can still run them -- by
-- default a plpgsql function runs as SECURITY INVOKER, i.e. with the
-- caller's privileges, which would make table-less roles fail. search_path
-- is pinned to prevent a caller-controlled search_path from shadowing the
-- jobs table with an object from another schema (CVE-2018-1058 pattern).
ALTER FUNCTION enqueue(TEXT, JSONB, INT, TIMESTAMPTZ) SECURITY DEFINER SET search_path = public, pg_catalog;
ALTER FUNCTION acquire_next(TEXT) SECURITY DEFINER SET search_path = public, pg_catalog;
ALTER FUNCTION mark_completed(BIGINT, BIGINT) SECURITY DEFINER SET search_path = public, pg_catalog;
ALTER FUNCTION mark_failed(BIGINT, BIGINT) SECURITY DEFINER SET search_path = public, pg_catalog;
ALTER FUNCTION reap_stuck(FLOAT) SECURITY DEFINER SET search_path = public, pg_catalog;
ALTER FUNCTION inspect_job(BIGINT) SECURITY DEFINER SET search_path = public, pg_catalog;
ALTER FUNCTION cancel_job(BIGINT) SECURITY DEFINER SET search_path = public, pg_catalog;

-- Two roles, split by which side of the queue they're allowed to act on.
-- A leaked producer credential (e.g. shipped inside a Python client) should
-- never be able to acquire/complete/fail a job or run the reaper, and a
-- worker should never need to enqueue/cancel/edit/delete on a client's behalf.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'aqueduct_producer') THEN
        CREATE ROLE aqueduct_producer LOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'aqueduct_worker') THEN
        CREATE ROLE aqueduct_worker LOGIN;
    END IF;
END;
$$;

-- Set passwords out of band (e.g. `ALTER ROLE aqueduct_producer WITH PASSWORD '...'`)
-- rather than committing them here.

GRANT CONNECT ON DATABASE aqueduct TO aqueduct_producer, aqueduct_worker;
GRANT USAGE ON SCHEMA public TO aqueduct_producer, aqueduct_worker;

-- Explicit, even though new roles have no table privileges by default --
-- this documents the intent and stays correct if that default ever changes.
REVOKE ALL ON jobs FROM aqueduct_producer, aqueduct_worker;

-- Postgres grants EXECUTE on new functions to PUBLIC by default, which would
-- let either role call every function regardless of the grants below. Strip
-- that default first so the per-role grants are the only path in.
REVOKE EXECUTE ON FUNCTION enqueue(TEXT, JSONB, INT, TIMESTAMPTZ) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION acquire_next(TEXT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION mark_completed(BIGINT, BIGINT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION mark_failed(BIGINT, BIGINT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION reap_stuck(FLOAT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION inspect_job(BIGINT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION cancel_job(BIGINT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION update_job(BIGINT, JSONB, TIMESTAMPTZ, INT, TEXT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION delete_job(BIGINT) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION enqueue(TEXT, JSONB, INT, TIMESTAMPTZ) TO aqueduct_producer;
GRANT EXECUTE ON FUNCTION cancel_job(BIGINT) TO aqueduct_producer;
GRANT EXECUTE ON FUNCTION inspect_job(BIGINT) TO aqueduct_producer;
GRANT EXECUTE ON FUNCTION update_job(BIGINT, JSONB, TIMESTAMPTZ, INT, TEXT) TO aqueduct_producer;
GRANT EXECUTE ON FUNCTION delete_job(BIGINT) TO aqueduct_producer;

GRANT EXECUTE ON FUNCTION acquire_next(TEXT) TO aqueduct_worker;
GRANT EXECUTE ON FUNCTION mark_completed(BIGINT, BIGINT) TO aqueduct_worker;
GRANT EXECUTE ON FUNCTION mark_failed(BIGINT, BIGINT) TO aqueduct_worker;
GRANT EXECUTE ON FUNCTION reap_stuck(FLOAT) TO aqueduct_worker;
