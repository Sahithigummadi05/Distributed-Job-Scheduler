package com.sahithi.jobscheduler.worker;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.handler.JobHandler;
import com.sahithi.jobscheduler.repository.JobRepository;
import com.sahithi.jobscheduler.retry.RetryPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Runs a single claimed job and applies the outcome: success, retry-with-backoff, or
 * dead-letter once {@code max_attempts} is exhausted. Kept separate from {@link JobPoller} so
 * the decision logic here is unit-testable without a polling loop or a database.
 */
@Component
public class JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);

    private final JobRepository repository;
    private final RetryPolicy retryPolicy;
    private final Map<String, JobHandler> handlersByType;
    private final MeterRegistry meterRegistry;

    // Explicit @Autowired: with more than one constructor, Spring won't infer which to use.
    @Autowired
    public JobExecutor(
            JobRepository repository,
            RetryPolicy retryPolicy,
            List<JobHandler> handlers,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
        this.meterRegistry = meterRegistry;
        this.handlersByType = handlers.stream()
                .collect(Collectors.toMap(JobHandler::jobType, Function.identity()));
        log.info("Registered {} job handler(s): {}", handlersByType.size(), handlersByType.keySet());
    }

    /** Convenience constructor for tests that don't care about metrics. */
    public JobExecutor(JobRepository repository, RetryPolicy retryPolicy, List<JobHandler> handlers) {
        this(repository, retryPolicy, handlers, new SimpleMeterRegistry());
    }

    public void execute(Job job) {
        var handler = handlersByType.get(job.jobType());
        if (handler == null) {
            // An unregistered job type will never succeed no matter how many times it's retried,
            // so it goes straight to the dead-letter queue instead of burning through attempts.
            log.error("No handler registered for job type '{}' (job {})", job.jobType(), job.id());
            repository.markDeadLetter(job.id(), "No handler registered for job type: " + job.jobType());
            count("jobs.deadlettered", job.jobType());
            return;
        }

        var sample = Timer.start(meterRegistry);
        try {
            handler.handle(job);
            repository.markSucceeded(job.id());
            count("jobs.succeeded", job.jobType());
            log.info("Job {} ({}) succeeded on attempt {}", job.id(), job.jobType(), job.attempts());
        } catch (Exception e) {
            handleFailure(job, e);
        } finally {
            // Records handler duration whichever way the attempt went, so a job type that is slow
            // only when it fails is still visible in the timing data.
            sample.stop(meterRegistry.timer("jobs.execution.duration", "jobType", job.jobType()));
        }
    }

    private void count(String metric, String jobType) {
        meterRegistry.counter(metric, "jobType", jobType).increment();
    }

    private void handleFailure(Job job, Exception e) {
        var error = e.getClass().getSimpleName() + ": " + e.getMessage();

        if (!job.hasAttemptsRemaining()) {
            log.error("Job {} ({}) exhausted {} attempts, moving to dead-letter queue",
                    job.id(), job.jobType(), job.maxAttempts(), e);
            repository.markDeadLetter(job.id(), error);
            count("jobs.deadlettered", job.jobType());
            return;
        }

        var delay = retryPolicy.nextDelay(job.attempts());
        var nextRunAt = Instant.now().plus(delay);
        log.warn("Job {} ({}) failed on attempt {}/{}, retrying in {}ms",
                job.id(), job.jobType(), job.attempts(), job.maxAttempts(), delay.toMillis(), e);
        repository.rescheduleAfterFailure(job.id(), error, nextRunAt);
        count("jobs.retried", job.jobType());
    }
}
