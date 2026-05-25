CREATE INDEX jobs_pending_queue_run_at ON jobs (queue, run_at) WHERE status = 'pending';
CREATE INDEX jobs_running_locked_at ON jobs (locked_at) WHERE status = 'running';