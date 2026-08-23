-- The lease reaper scans RUNNING rows by locked_at. Without this it degrades to a sequential
-- scan of the whole table on every sweep; a partial index keeps it proportional to the number of
-- in-flight jobs (small) rather than total job history (unbounded).
CREATE INDEX idx_jobs_running_leases ON jobs (locked_at)
    WHERE status = 'RUNNING';
