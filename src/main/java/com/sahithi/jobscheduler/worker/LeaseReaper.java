package com.sahithi.jobscheduler.worker;

import com.sahithi.jobscheduler.config.SchedulerProperties;
import com.sahithi.jobscheduler.repository.JobRepository;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically returns jobs abandoned by dead workers to the queue.
 *
 * <p>Every worker runs its own reaper - there is deliberately no "the reaper" instance to elect or
 * keep alive, since a single designated reaper would itself be a single point of failure (and if
 * it were the process that died, nothing would recover its jobs). Concurrent sweeps are safe
 * because the reclaim query uses SKIP LOCKED.
 *
 * <p>The lease timeout must exceed the longest a legitimate job can run, or a slow job will be
 * reclaimed and executed a second time while the first run is still going.
 */
@Component
@ConditionalOnProperty(name = "scheduler.polling-enabled", havingValue = "true", matchIfMissing = true)
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobRepository repository;
    private final Duration leaseTimeout;

    public LeaseReaper(JobRepository repository, SchedulerProperties properties) {
        this.repository = repository;
        this.leaseTimeout = Duration.ofSeconds(properties.leaseTimeoutSeconds());
    }

    @Scheduled(fixedDelayString = "${scheduler.reaper-interval-ms:30000}")
    public void reclaimOrphanedJobs() {
        try {
            repository.reclaimExpiredLeases(leaseTimeout);
        } catch (Exception e) {
            // Never let a sweep failure kill the scheduled task - Spring stops re-invoking a
            // @Scheduled method that throws, which would silently disable orphan recovery.
            log.error("Lease reaper sweep failed", e);
        }
    }
}
