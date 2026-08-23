package com.sahithi.jobscheduler.worker;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.handler.JobHandler;
import com.sahithi.jobscheduler.repository.JobRepository;
import com.sahithi.jobscheduler.retry.RetryPolicy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public JobExecutor(JobRepository repository, RetryPolicy retryPolicy, List<JobHandler> handlers) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
        this.handlersByType = handlers.stream()
                .collect(Collectors.toMap(JobHandler::jobType, Function.identity()));
        log.info("Registered {} job handler(s): {}", handlersByType.size(), handlersByType.keySet());
    }

    public void execute(Job job) {
        var handler = handlersByType.get(job.jobType());
        if (handler == null) {
            // An unregistered job type will never succeed no matter how many times it's retried,
            // so it goes straight to the dead-letter queue instead of burning through attempts.
            log.error("No handler registered for job type '{}' (job {})", job.jobType(), job.id());
            repository.markDeadLetter(job.id(), "No handler registered for job type: " + job.jobType());
            return;
        }

        try {
            handler.handle(job);
            repository.markSucceeded(job.id());
            log.info("Job {} ({}) succeeded on attempt {}", job.id(), job.jobType(), job.attempts());
        } catch (Exception e) {
            handleFailure(job, e);
        }
    }

    private void handleFailure(Job job, Exception e) {
        var error = e.getClass().getSimpleName() + ": " + e.getMessage();

        if (!job.hasAttemptsRemaining()) {
            log.error("Job {} ({}) exhausted {} attempts, moving to dead-letter queue",
                    job.id(), job.jobType(), job.maxAttempts(), e);
            repository.markDeadLetter(job.id(), error);
            return;
        }

        var delay = retryPolicy.nextDelay(job.attempts());
        var nextRunAt = Instant.now().plus(delay);
        log.warn("Job {} ({}) failed on attempt {}/{}, retrying in {}ms",
                job.id(), job.jobType(), job.attempts(), job.maxAttempts(), delay.toMillis(), e);
        repository.rescheduleAfterFailure(job.id(), error, nextRunAt);
    }
}
