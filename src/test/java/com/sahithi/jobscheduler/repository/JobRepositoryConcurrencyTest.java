package com.sahithi.jobscheduler.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.domain.JobStatus;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

/**
 * The central correctness property of this project: many workers polling the same table
 * concurrently must each get a disjoint set of jobs - no job processed twice, none lost.
 *
 * These run against a real PostgreSQL instance rather than an embedded database on purpose:
 * {@code FOR UPDATE SKIP LOCKED} is the entire mechanism under test, and H2/HSQLDB do not
 * implement it faithfully. A test that "passed" on an embedded database would be proving nothing.
 */
@SpringBootTest
@ActiveProfiles("test")
class JobRepositoryConcurrencyTest {

    @Autowired
    private JobRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearJobs() {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
    }

    @Test
    @DisplayName("20 concurrent workers claiming 500 jobs each get disjoint sets - no duplicates, none lost")
    void concurrentWorkersNeverClaimTheSameJobTwice() throws Exception {
        final var jobCount = 500;
        final var workerCount = 20;
        final var batchSize = 10;

        for (var i = 0; i < jobCount; i++) {
            repository.enqueue("test.job", "{\"n\":" + i + "}", 0, 3, null);
        }

        var allClaimed = new ConcurrentLinkedQueue<UUID>();
        var startGate = new CountDownLatch(1);
        var doneGate = new CountDownLatch(workerCount);
        var pool = Executors.newFixedThreadPool(workerCount);

        for (var w = 0; w < workerCount; w++) {
            var workerId = "worker-" + w;
            pool.submit(() -> {
                try {
                    startGate.await(); // release all workers at once to maximize contention
                    List<Job> claimed;
                    do {
                        claimed = repository.claimBatch(workerId, batchSize);
                        claimed.forEach(job -> allClaimed.add(job.id()));
                    } while (!claimed.isEmpty());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(doneGate.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // Every job claimed exactly once: no duplicates (SKIP LOCKED did its job) and none
        // dropped (every PENDING row was eventually visible to some worker).
        assertThat(allClaimed).hasSize(jobCount);
        assertThat(Set.copyOf(allClaimed)).hasSize(jobCount);

        var runningCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING'", Integer.class);
        assertThat(runningCount).isEqualTo(jobCount);
    }

    @Test
    @DisplayName("claimBatch increments attempts and stamps the owning worker")
    void claimingMarksTheJobRunningAndRecordsTheWorker() {
        repository.enqueue("test.job", "{}", 0, 3, null);

        var claimed = repository.claimBatch("worker-a", 10);

        assertThat(claimed).hasSize(1);
        var job = claimed.get(0);
        assertThat(job.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(job.lockedBy()).isEqualTo("worker-a");
        assertThat(job.attempts()).isEqualTo(1);
        assertThat(job.lockedAt()).isNotNull();
    }

    @Test
    @DisplayName("a job scheduled in the future is not claimable yet")
    void futureScheduledJobsAreNotClaimed() {
        var id = repository.enqueue("test.job", "{}", 0, 3, null);
        jdbcTemplate.update("UPDATE jobs SET next_run_at = now() + interval '1 hour' WHERE id = ?", id);

        assertThat(repository.claimBatch("worker-a", 10)).isEmpty();
    }

    @Test
    @DisplayName("higher priority jobs are claimed first")
    void higherPriorityJobsAreClaimedFirst() {
        repository.enqueue("test.job", "{\"p\":\"low\"}", 0, 3, null);
        var highId = repository.enqueue("test.job", "{\"p\":\"high\"}", 10, 3, null);

        var claimed = repository.claimBatch("worker-a", 1);

        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).id()).isEqualTo(highId);
    }

    @Test
    @DisplayName("re-enqueuing the same dedupe key returns the original job instead of duplicating it")
    void dedupeKeyPreventsDoubleEnqueue() {
        var first = repository.enqueue("test.job", "{}", 0, 3, "order-123");
        var second = repository.enqueue("test.job", "{}", 0, 3, "order-123");

        assertThat(second).isEqualTo(first);
        var total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM jobs", Integer.class);
        assertThat(total).isEqualTo(1);
    }

    @Test
    @DisplayName("jobs without a dedupe key can be enqueued repeatedly")
    void nullDedupeKeysDoNotCollide() {
        repository.enqueue("test.job", "{}", 0, 3, null);
        repository.enqueue("test.job", "{}", 0, 3, null);

        var total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM jobs", Integer.class);
        assertThat(total).isEqualTo(2);
    }
}
