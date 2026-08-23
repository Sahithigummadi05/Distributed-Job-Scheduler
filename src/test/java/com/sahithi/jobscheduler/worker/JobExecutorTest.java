package com.sahithi.jobscheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.domain.JobStatus;
import com.sahithi.jobscheduler.handler.JobHandler;
import com.sahithi.jobscheduler.repository.JobRepository;
import com.sahithi.jobscheduler.retry.RetryPolicy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

/**
 * End-to-end lifecycle behavior against a real database: a failing job retries with backoff
 * until its attempts are exhausted, then lands in the dead-letter queue rather than looping
 * forever or vanishing.
 */
@SpringBootTest
@ActiveProfiles("test")
class JobExecutorTest {

    @Autowired
    private JobRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private RetryPolicy fastRetryPolicy;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
        // Millisecond-scale backoff so the retry path is exercised without the test sleeping.
        fastRetryPolicy = new RetryPolicy(Duration.ofMillis(1), Duration.ofMillis(5));
    }

    @Test
    @DisplayName("a succeeding handler marks the job SUCCEEDED")
    void successfulJobIsMarkedSucceeded() {
        var handled = new AtomicInteger();
        var executor = executorFor(job -> handled.incrementAndGet());

        var id = repository.enqueue("ok.job", "{}", 0, 3, null);
        executor.execute(repository.claimBatch("w1", 1).get(0));

        assertThat(handled.get()).isEqualTo(1);
        assertThat(repository.findById(id)).get().extracting(Job::status).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    @DisplayName("a failing job is rescheduled as PENDING with the error recorded, until attempts run out")
    void failingJobRetriesThenDeadLetters() throws Exception {
        var attempts = new AtomicInteger();
        var executor = executorFor(job -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("handler blew up");
        });

        var maxAttempts = 3;
        var id = repository.enqueue("failing.job", "{}", 0, maxAttempts, null);

        for (var attempt = 1; attempt <= maxAttempts; attempt++) {
            var claimed = repository.claimBatch("w1", 1);
            assertThat(claimed).as("attempt %d should find the job claimable", attempt).hasSize(1);
            executor.execute(claimed.get(0));

            var job = repository.findById(id).orElseThrow();
            if (attempt < maxAttempts) {
                assertThat(job.status()).isEqualTo(JobStatus.PENDING);
                assertThat(job.lastError()).contains("handler blew up");
                assertThat(job.lockedBy()).isNull();
                // Wait out the (millisecond) backoff so the next claim sees it as due.
                Thread.sleep(10);
            } else {
                assertThat(job.status()).isEqualTo(JobStatus.DEAD_LETTER);
            }
        }

        assertThat(attempts.get()).isEqualTo(maxAttempts);
        var job = repository.findById(id).orElseThrow();
        assertThat(job.attempts()).isEqualTo(maxAttempts);
        assertThat(job.lastError()).contains("IllegalStateException");
    }

    @Test
    @DisplayName("a job whose type has no registered handler goes straight to dead-letter")
    void unknownJobTypeDeadLettersImmediately() {
        var executor = executorFor(job -> {});

        var id = repository.enqueue("nobody.handles.this", "{}", 0, 5, null);
        executor.execute(repository.claimBatch("w1", 1).get(0));

        var job = repository.findById(id).orElseThrow();
        // Retrying would be pointless - no amount of attempts registers a missing handler.
        assertThat(job.status()).isEqualTo(JobStatus.DEAD_LETTER);
        assertThat(job.attempts()).isEqualTo(1);
        assertThat(job.lastError()).contains("No handler registered");
    }

    /** Builds an executor whose only handler responds to the job type used by that test. */
    private JobExecutor executorFor(ThrowingConsumer handlerBody) {
        return new JobExecutor(repository, fastRetryPolicy, List.of(
                handlerFor("ok.job", handlerBody),
                handlerFor("failing.job", handlerBody)));
    }

    private JobHandler handlerFor(String type, ThrowingConsumer body) {
        return new JobHandler() {
            @Override
            public String jobType() {
                return type;
            }

            @Override
            public void handle(Job job) throws Exception {
                body.accept(job);
            }
        };
    }

    @FunctionalInterface
    private interface ThrowingConsumer {
        void accept(Job job) throws Exception;
    }
}
