# Distributed Job Scheduler

![CI](https://github.com/Sahithigummadi05/project2/actions/workflows/ci.yml/badge.svg)

A horizontally-scalable background job scheduler in Java 21 / Spring Boot, where **many worker
instances poll one PostgreSQL database and provably never process the same job twice** — no
message broker, no distributed lock service, no leader election.

Coordination is done in SQL, using `SELECT ... FOR UPDATE SKIP LOCKED`, the same approach
production job queues built on Postgres use (Oban, pg-boss, GoodJob, Sidekiq's Postgres variants).

## The core problem, and proof it's actually solved

If N workers poll the same table, the obvious implementation is:

```sql
SELECT id FROM jobs WHERE status = 'PENDING' LIMIT 10;   -- read candidates
UPDATE jobs SET status = 'RUNNING' WHERE id IN (...);    -- claim them
```

This is **wrong**, and it's wrong in a way that's invisible in single-worker testing: between the
`SELECT` and the `UPDATE` no lock is held, so two workers read the same rows and both "claim" them.
Every job runs multiple times — charging a customer twice, sending duplicate emails, etc.

The fix is to take the lock and claim in one atomic statement, telling Postgres to skip rows
another transaction already holds:

```sql
WITH claimed AS (
    SELECT id FROM jobs
    WHERE status = 'PENDING' AND next_run_at <= now()
    ORDER BY priority DESC, created_at ASC
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED          -- <- the load-bearing clause
)
UPDATE jobs SET status = 'RUNNING', locked_by = :workerId, attempts = attempts + 1
WHERE id IN (SELECT id FROM claimed)
RETURNING *;
```

**Both versions are in the test suite, run against real PostgreSQL under identical contention:**

| Implementation | Workers | Jobs | Total claims | Duplicate claims |
|---|---|---|---|---|
| Naive `SELECT`-then-`UPDATE` | 16 | 300 | 1,890 | **1,590** |
| `FOR UPDATE SKIP LOCKED` | 20 | 500 | 500 | **0** |

`NaiveClaimingIsUnsafeTest` asserts the broken version *does* double-claim; that's deliberate.
A concurrency test that would pass with or without the mechanism it's meant to verify proves
nothing — this one demonstrably has teeth.

## Features

- **Exactly-once claiming** across arbitrarily many worker instances (the table above).
- **Retries with exponential backoff + full jitter** — `random(0, min(cap, base·2ⁿ))`, the AWS
  algorithm, so a batch of jobs failing together doesn't retry in lockstep (thundering herd).
  Capped so a repeatedly-failing job isn't scheduled years out; overflow-guarded for large N.
- **Dead-letter queue** — jobs that exhaust `max_attempts` land in `DEAD_LETTER` for inspection
  instead of retrying forever or disappearing.
- **Idempotent enqueue** — an optional `dedupeKey` (partial unique index + `ON CONFLICT DO NOTHING`)
  makes re-submitting the same logical job return the original job's id rather than duplicating it.
- **Priority scheduling and delayed jobs** — `priority DESC` ordering, `next_run_at` for
  scheduling work in the future.
- **REST API** for enqueueing and inspecting jobs, plus queue-depth stats.
- **Health/readiness/liveness probes** via Spring Actuator — ready for a Kubernetes deployment.
- **Graceful shutdown** — in-flight jobs drain (up to 30s) before the worker pool closes.

## Architecture

```
   POST /api/jobs
         │
         ▼
   ┌───────────┐        claim (SKIP LOCKED)        ┌──────────────┐
   │           │◄──────────────────────────────────│  Worker #1   │
   │ PostgreSQL│                                   │ poll ▸ exec  │
   │  jobs     │◄──────────────────────────────────│  Worker #2   │
   │  table    │                                   │  ...         │
   │           │◄──────────────────────────────────│  Worker #N   │
   └───────────┘      each gets disjoint rows      └──────────────┘
```

The database is the coordination primitive. Workers are stateless and interchangeable — scale by
running more of them, with no configuration change and nothing to elect.

| Package | Responsibility |
|---|---|
| `repository` | All coordination SQL, including the atomic claim |
| `retry` | `RetryPolicy` — pure backoff-with-jitter function, no Spring/clock/IO |
| `worker` | `JobPoller` (polling loop, bounded thread pool) and `JobExecutor` (run + outcome) |
| `handler` | `JobHandler` interface — implement + register as a bean to handle a job type |
| `api` | REST controller |

## Running it

```bash
docker compose up --build            # Postgres + 2 workers
docker compose up --scale worker=5   # ...or 5, no config change needed
```

Then:

```bash
# Enqueue a job
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"jobType":"echo","payload":"{\"message\":\"hello\"}","priority":5,"dedupeKey":"demo-1"}'
# -> {"id":"4f7418dd-3968-48e9-be9b-5597d03901f5"}

# Re-submitting the same dedupeKey returns the SAME id - no duplicate job
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' -d '{"jobType":"echo","dedupeKey":"demo-1"}'
# -> {"id":"4f7418dd-3968-48e9-be9b-5597d03901f5"}

curl http://localhost:8080/api/jobs/stats            # queue depth by status
curl 'http://localhost:8080/api/jobs?status=DEAD_LETTER'   # inspect failures
curl http://localhost:8080/actuator/health           # liveness/readiness
```

## Adding a job type

```java
@Component
public class SendEmailHandler implements JobHandler {
    @Override public String jobType() { return "send-email"; }

    @Override public void handle(Job job) throws Exception {
        // throwing here => retry with backoff, then dead-letter once attempts run out
    }
}
```

## Testing

```bash
mvn verify
```

20 tests, run in CI on every push against a real PostgreSQL service container.

Tests run against **real PostgreSQL, never an embedded database** — `FOR UPDATE SKIP LOCKED` is
the entire mechanism under test and H2/HSQLDB don't implement it faithfully, so a green test on an
embedded database would be measuring nothing. The suite covers: the 20-worker concurrency
property, the naive-implementation counter-proof, retry/backoff bounds and jitter distribution,
overflow safety at large attempt counts, the full retry → dead-letter lifecycle, unknown-handler
handling, dedupe-key idempotency, priority ordering, and future-scheduled jobs not being claimed early.

## Tech

Java 21 · Spring Boot 3.3 · PostgreSQL 16 · Flyway · JUnit 5 / AssertJ · Maven · Docker · GitHub Actions
