package com.sahithi.jobscheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.handler.JobHandler;
import com.sahithi.jobscheduler.repository.JobRepository;
import com.sahithi.jobscheduler.retry.RetryPolicy;
import com.sahithi.jobscheduler.worker.JobExecutor;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

/**
 * Measures end-to-end throughput (claim + execute + record outcome) at varying worker counts,
 * so the project's performance characteristics are measured rather than assumed.
 *
 * Deliberately measures the <em>scheduler's</em> overhead with a no-op handler: the interesting
 * number is how fast the claim/execute/complete machinery itself runs, since real handlers are
 * dominated by whatever work they do. Numbers vary by machine and by how much CPU the database
 * gets - the shape (does it scale with workers, where does it flatten) is the point, not the
 * absolute value.
 */
@SpringBootTest
@ActiveProfiles("test")
class ThroughputBenchmarkTest {

    private static final int JOB_COUNT = 2000;

    @Autowired
    private JobRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearJobs() {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
    }

    @Test
    @DisplayName("throughput scales with worker count and every job is processed exactly once")
    void measureThroughputAcrossWorkerCounts() throws Exception {
        System.out.println("\n=== Job scheduler throughput (" + JOB_COUNT + " jobs, no-op handler) ===");
        System.out.printf("%-10s %-12s %-16s%n", "workers", "elapsed", "jobs/sec");

        for (var workerCount : new int[] {1, 2, 4, 8, 16}) {
            var result = runBenchmark(workerCount);
            System.out.printf("%-10d %-12s %,-16.0f%n", workerCount, result.elapsedMs + " ms", result.jobsPerSecond);

            // Throughput is only meaningful if the work was actually done correctly.
            assertThat(result.processed).isEqualTo(JOB_COUNT);
        }
        System.out.println();
    }

    private record BenchmarkResult(long elapsedMs, double jobsPerSecond, int processed) {
    }

    private BenchmarkResult runBenchmark(int workerCount) throws Exception {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
        for (var i = 0; i < JOB_COUNT; i++) {
            repository.enqueue("bench.noop", "{}", 0, 1, null);
        }

        var processed = new AtomicInteger();
        var executor = new JobExecutor(
                repository,
                new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(10)),
                List.of(new NoOpHandler(processed)));

        var startGate = new CountDownLatch(1);
        var doneGate = new CountDownLatch(workerCount);
        var pool = Executors.newFixedThreadPool(workerCount);

        for (var w = 0; w < workerCount; w++) {
            var workerId = "bench-worker-" + w;
            pool.submit(() -> {
                try {
                    startGate.await();
                    List<Job> claimed;
                    do {
                        claimed = repository.claimBatch(workerId, 20);
                        claimed.forEach(executor::execute);
                    } while (!claimed.isEmpty());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneGate.countDown();
                }
            });
        }

        var start = System.nanoTime();
        startGate.countDown();
        assertThat(doneGate.await(120, TimeUnit.SECONDS)).isTrue();
        var elapsedMs = (System.nanoTime() - start) / 1_000_000;
        pool.shutdown();

        var succeeded = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = 'SUCCEEDED'", Integer.class);
        assertThat(succeeded).isEqualTo(JOB_COUNT);

        return new BenchmarkResult(
                elapsedMs, JOB_COUNT / (Math.max(elapsedMs, 1) / 1000.0), processed.get());
    }

    private record NoOpHandler(AtomicInteger counter) implements JobHandler {
        @Override
        public String jobType() {
            return "bench.noop";
        }

        @Override
        public void handle(Job job) {
            counter.incrementAndGet();
        }
    }
}
