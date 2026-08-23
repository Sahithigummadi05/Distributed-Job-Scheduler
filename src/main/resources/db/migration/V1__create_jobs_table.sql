CREATE TABLE jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type      VARCHAR(100) NOT NULL,
    payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    status        VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTER')),
    priority      INT NOT NULL DEFAULT 0,
    attempts      INT NOT NULL DEFAULT 0,
    max_attempts  INT NOT NULL DEFAULT 5,
    next_run_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by     VARCHAR(100),
    locked_at     TIMESTAMPTZ,
    dedupe_key    VARCHAR(200),
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim query filters on (status, next_run_at) and orders by priority - this index is what
-- keeps SELECT ... FOR UPDATE SKIP LOCKED an index scan instead of a sequential scan once the
-- table has real volume.
CREATE INDEX idx_jobs_claimable ON jobs (status, next_run_at, priority DESC)
    WHERE status = 'PENDING';

-- Submitting the same logical job twice (e.g. a retried HTTP request from the caller) should not
-- enqueue it twice; NULL dedupe keys are allowed to repeat freely (a partial unique index ignores
-- NULLs by default in Postgres, exactly the behavior wanted here).
CREATE UNIQUE INDEX idx_jobs_dedupe_key ON jobs (dedupe_key) WHERE dedupe_key IS NOT NULL;

CREATE INDEX idx_jobs_status ON jobs (status);
