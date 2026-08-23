package com.sahithi.jobscheduler.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.sahithi.jobscheduler.domain.JobStatus;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

/**
 * A worker that dies mid-job (OOM kill, pod eviction, power loss) never gets to write an outcome,
 * so its job is left {@code RUNNING} with a {@code locked_by} that will never come back. Without
 * recovery those jobs are silently lost forever - the queue looks healthy while work quietly
 * disappears.
 *
 * The fix is lease-based: a claim is only valid for a bounded time, and any {@code RUNNING} job
 * whose lease has expired is assumed abandoned and returned to the queue.
 */
@SpringBootTest
@ActiveProfiles("test")
class OrphanedJobRecoveryTest {

    @Autowired
    private JobRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearJobs() {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
    }

    /** Simulates a crash: the job was claimed, but the worker died before writing an outcome. */
    private void simulateCrashedWorker(java.util.UUID jobId, Duration ago) {
        jdbcTemplate.update(
                "UPDATE jobs SET locked_at = now() - CAST(? AS interval) WHERE id = ?",
                ago.toSeconds() + " seconds",
                jobId);
    }

    @Test
    @DisplayName("a job orphaned by a crashed worker is returned to PENDING and runs again")
    void orphanedJobIsReclaimed() {
        var id = repository.enqueue("test.job", "{}", 0, 3, null);
        var claimed = repository.claimBatch("worker-that-will-crash", 1);
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).status()).isEqualTo(JobStatus.RUNNING);

        simulateCrashedWorker(id, Duration.ofMinutes(10));

        var reclaimed = repository.reclaimExpiredLeases(Duration.ofMinutes(5));

        assertThat(reclaimed).isEqualTo(1);
        var job = repository.findById(id).orElseThrow();
        assertThat(job.status()).isEqualTo(JobStatus.PENDING);
        assertThat(job.lockedBy()).isNull();
        assertThat(job.lastError()).contains("lease expired");
        // The job is claimable again, so the work is not lost.
        assertThat(repository.claimBatch("healthy-worker", 1)).hasSize(1);
    }

    @Test
    @DisplayName("a job still within its lease is left alone - a slow job is not a dead one")
    void inFlightJobIsNotReclaimed() {
        repository.enqueue("test.job", "{}", 0, 3, null);
        repository.claimBatch("busy-worker", 1);

        // Claimed just now, so well inside a 5-minute lease.
        var reclaimed = repository.reclaimExpiredLeases(Duration.ofMinutes(5));

        assertThat(reclaimed).isZero();
        var running = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING'", Integer.class);
        assertThat(running).isEqualTo(1);
    }

    @Test
    @DisplayName("an orphaned job that has already exhausted its attempts dead-letters instead of looping")
    void orphanedJobWithNoAttemptsLeftGoesToDeadLetter() {
        // max_attempts = 1, so the single claim below consumes the only attempt.
        var id = repository.enqueue("test.job", "{}", 0, 1, null);
        repository.claimBatch("worker-that-will-crash", 1);
        simulateCrashedWorker(id, Duration.ofMinutes(10));

        var reclaimed = repository.reclaimExpiredLeases(Duration.ofMinutes(5));

        assertThat(reclaimed).isEqualTo(1);
        var job = repository.findById(id).orElseThrow();
        // Requeueing forever would let a job that reliably kills its worker (a "poison pill")
        // take down every worker in the fleet in turn.
        assertThat(job.status()).isEqualTo(JobStatus.DEAD_LETTER);
    }

    @Test
    @DisplayName("recovery is safe to run concurrently from every worker")
    void concurrentReclaimDoesNotDoubleCount() throws Exception {
        for (var i = 0; i < 50; i++) {
            var id = repository.enqueue("test.job", "{}", 0, 3, null);
            repository.claimBatch("crashed", 1);
            simulateCrashedWorker(id, Duration.ofMinutes(10));
        }

        // Every worker runs the reaper on its own schedule, so it must be idempotent under
        // concurrency - the same SKIP LOCKED property the claim query relies on.
        var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        var total = new java.util.concurrent.atomic.AtomicInteger();
        var latch = new java.util.concurrent.CountDownLatch(8);
        for (var i = 0; i < 8; i++) {
            pool.submit(() -> {
                try {
                    total.addAndGet(repository.reclaimExpiredLeases(Duration.ofMinutes(5)));
                } finally {
                    latch.countDown();
                }
            });
        }
        assertThat(latch.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // Exactly 50 reclaims in total - no job recovered twice.
        assertThat(total.get()).isEqualTo(50);
    }
}
