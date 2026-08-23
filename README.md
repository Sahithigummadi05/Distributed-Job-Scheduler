# Distributed Job Scheduler

![CI](https://github.com/Sahithigummadi05/project2/actions/workflows/ci.yml/badge.svg)

A horizontally-scalable background job scheduler in Java 21 / Spring Boot, where **many worker
instances poll one PostgreSQL database and provably never claim the same job concurrently** — no
message broker, no distributed lock service, no leader election.

Delivery is **at-least-once**, not exactly-once. That distinction is deliberate and explained in
[Delivery semantics](#delivery-semantics-at-least-once-not-exactly-once) below.

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

## Surviving worker crashes

Safe claiming alone isn't enough. If a worker is killed mid-job — OOM kill, pod eviction, power
loss — it never writes an outcome, so its rows sit in `RUNNING` with a `locked_by` that will never
return. The queue looks healthy while work silently disappears.

So a claim is a **lease**, not a permanent assignment. Every worker runs a reaper that returns
jobs whose lease expired:

```sql
WITH expired AS (
    SELECT id FROM jobs
    WHERE status = 'RUNNING' AND locked_at < now() - make_interval(secs => :timeout)
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = CASE WHEN attempts < max_attempts THEN 'PENDING' ELSE 'DEAD_LETTER' END, ...
```

Three design decisions worth noting:

- **Every worker reaps; there is no elected reaper.** A single designated reaper would be a
  single point of failure — and if *it* were the process that died, nothing would recover its
  jobs. Concurrent sweeps are safe for the same reason claiming is: `SKIP LOCKED`.
- **An orphan with no attempts left is dead-lettered, not requeued.** A job that reliably kills
  whichever worker picks it up (a "poison pill") would otherwise take down the fleet one worker
  at a time.
- **The lease timeout must exceed your slowest legitimate job**, or a slow job gets reclaimed and
  run a second time while the first attempt is still in flight. That trade-off is the price of
  crash recovery without a heartbeat protocol, and it's documented in the config.

## Delivery semantics: at-least-once, not exactly-once

**Claiming is exactly-once. Execution is at-least-once.** Those are different guarantees and the
distinction matters, so it's worth being precise about what this system does and does not promise.

The claim query guarantees no two workers ever hold the same job at the same time — that's the
property the concurrency tests verify. But consider this interleaving:

1. Worker A claims job X and runs it to completion — the email is sent, the payment is charged.
2. Worker A is killed *before* it can write `status = 'SUCCEEDED'`.
3. The lease expires. The reaper sees a `RUNNING` job with no live owner and requeues it.
4. Worker B claims job X and runs it again. The email is sent twice.

No amount of locking fixes this. The work (an external side effect) and the bookkeeping (a row
update) are in two different systems, so there is no atomic commit spanning both. Closing the gap
genuinely requires either a distributed transaction across your side effect and the database, or
transactional outbox/two-phase commit — both a large step up in complexity.

So this scheduler makes the same trade every mainstream job queue makes. **Sidekiq, Celery, AWS
SQS, and Oban are all at-least-once for exactly this reason.** The industry-standard resolution is
to push idempotency to the handler rather than pretend the queue can provide it:

```java
public void handle(Job job) {
    // Derive a stable key from the job, not from the attempt.
    if (alreadyProcessed(job.id())) return;
    chargeCustomer(...);
    recordProcessed(job.id());
}
```

**Handlers must be idempotent.** The scheduler makes that cheap — `job.id()` is stable across
retries and reclaims, so it works directly as a deduplication key. Enqueue-side idempotency is
handled separately by `dedupeKey` (a partial unique index), which prevents the same logical job
from entering the queue twice in the first place.

Anyone claiming exactly-once execution for a queue like this is either wrong or quietly redefining
"execution" to mean "claiming."

## Features

- **Exactly-once claiming** across arbitrarily many worker instances (the table above) —
  with at-least-once *execution*, per [Delivery semantics](#delivery-semantics-at-least-once-not-exactly-once).
- **Crash recovery** via lease expiry — jobs orphaned by dead workers are returned to the queue.
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
- **Metrics** via Micrometer — `jobs.succeeded`, `jobs.retried`, `jobs.deadlettered` (tagged by job
  type), execution-duration timers, and a `jobs.queue.depth` gauge per status. Backlog and
  dead-letter count are what you'd actually alert on.
- **Health/readiness/liveness probes** via Spring Actuator — ready for a Kubernetes deployment.
- **Graceful shutdown** — in-flight jobs drain (up to 30s) before the worker pool closes.

## Measured throughput

End-to-end (claim → execute → record outcome), 2,000 jobs with a no-op handler, so the numbers
reflect the scheduler's own overhead rather than whatever a real handler does. Run it yourself
with `mvn test -Dtest=ThroughputBenchmarkTest`.

Ranges below are the min–max across repeated consecutive runs on one containerized dev box
(shared CPU, PostgreSQL on the same host):

| Workers | Jobs/sec (observed range) |
|---:|---:|
| 1 | 1,200 – 3,200 |
| 2 | 2,400 – 5,600 |
| 4 | 4,100 – 8,100 |
| 8 | 4,100 – 10,800 |
| 16 | 4,500 – 12,600 |

**Ranges, not point values, because the spread is real:** identical back-to-back runs varied by up
to ~2.5×, and at 16 workers one run came in *below* its own 8-worker result. Reporting a single
flattering number here would be measurement theater — on shared infrastructure the absolute
figures say more about what else the box was doing than about the scheduler.

What *is* stable across every run is the shape:

- **Scaling is real but sub-linear.** 1 → 4 workers reliably gives roughly 3–3.5×.
- **It flattens past ~8 workers, then becomes noise-dominated.** With a no-op handler every worker
  contends on the same database, so the database saturates well before the workers do. Beyond that
  point, added workers buy contention rather than throughput.
- **Correctness holds at every level.** The benchmark asserts all 2,000 jobs completed exactly
  once at each worker count, so these measure *correct* execution, not just SQL round-trips.

For a real workload the useful conclusion isn't the headline number — it's that the bottleneck is
the database, so scaling past a handful of workers per database is the wrong lever.

Throughput scales with worker count, but **sub-linearly, and it flattens** — 16× the workers buys
roughly 4× the throughput. That's the expected shape, not a defect: with a no-op handler every
worker is contending on the same database, so the database becomes the bottleneck well before the
workers do. The benchmark asserts all 2,000 jobs completed exactly once at every worker count, so
these are throughput numbers for *correct* execution, not just SQL round-trips.

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
| `repository` | All coordination SQL — the atomic claim and the lease reclaim |
| `retry` | `RetryPolicy` — pure backoff-with-jitter function, no Spring/clock/IO |
| `worker` | `JobPoller` (polling loop), `JobExecutor` (run + outcome), `LeaseReaper` (crash recovery) |
| `handler` | `JobHandler` interface — implement + register as a bean to handle a job type |
| `api` | REST controller |
| `config` | Properties, bean wiring, queue-depth metrics |

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

# Metrics
curl http://localhost:8080/actuator/metrics/jobs.succeeded
curl http://localhost:8080/actuator/metrics/jobs.deadlettered
curl http://localhost:8080/actuator/metrics/jobs.queue.depth
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

31 tests, run in CI on every push against a real PostgreSQL service container.

Tests run against **real PostgreSQL, never an embedded database** — `FOR UPDATE SKIP LOCKED` is
the entire mechanism under test and H2/HSQLDB don't implement it faithfully, so a green test on an
embedded database would be measuring nothing. The suite covers:

- **Concurrency** — the 20-worker exactly-once property, plus the naive-implementation counter-proof
- **Crash recovery** — orphaned jobs reclaimed, in-flight jobs left alone, poison pills
  dead-lettered, and concurrent reaping recovering each job exactly once
- **Retry** — backoff bounds, jitter actually varying, overflow safety at large attempt counts,
  and the full retry → dead-letter lifecycle
- **Semantics** — dedupe-key idempotency, priority ordering, future-scheduled jobs not claimed
  early, unknown job types dead-lettering immediately rather than retrying pointlessly
- **API** — enqueue, validation, idempotency, lookup, filtering, stats
- **Throughput** — measured across 1–16 workers, asserting correctness at every level

## Tech

Java 21 · Spring Boot 3.3 · PostgreSQL 16 · Flyway · JUnit 5 / AssertJ · Maven · Docker · GitHub Actions
