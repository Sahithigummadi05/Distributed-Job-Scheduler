package com.sahithi.jobscheduler.worker;

import com.sahithi.jobscheduler.config.SchedulerProperties;
import com.sahithi.jobscheduler.repository.JobRepository;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Polls for claimable jobs on a fixed interval and hands each claimed job to a bounded thread
 * pool. Claiming is a single atomic SQL statement (see {@code JobRepository.claimBatch}), so
 * running many instances of this application against one database needs no extra coordination -
 * that's the property the concurrency test in this repo actually verifies.
 */
@Component
@ConditionalOnProperty(name = "scheduler.polling-enabled", havingValue = "true", matchIfMissing = true)
public class JobPoller {

    private static final Logger log = LoggerFactory.getLogger(JobPoller.class);

    private final JobRepository repository;
    private final JobExecutor executor;
    private final SchedulerProperties properties;
    private final ExecutorService workerPool;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public JobPoller(JobRepository repository, JobExecutor executor, SchedulerProperties properties) {
        this.repository = repository;
        this.executor = executor;
        this.properties = properties;
        this.workerPool = Executors.newFixedThreadPool(properties.executionThreads());
    }

    @Scheduled(fixedDelayString = "${scheduler.poll-interval-ms:500}")
    public void poll() {
        if (shuttingDown.get()) {
            return;
        }
        try {
            var claimed = repository.claimBatch(properties.workerId(), properties.batchSize());
            if (claimed.isEmpty()) {
                return;
            }
            log.debug("Worker {} claimed {} job(s)", properties.workerId(), claimed.size());

            // Wait for this batch before returning so the fixed-delay schedule can't stack
            // overlapping batches and overwhelm the pool when jobs run longer than the interval.
            var batchDone = new CountDownLatch(claimed.size());
            for (var job : claimed) {
                workerPool.submit(() -> {
                    try {
                        executor.execute(job);
                    } finally {
                        batchDone.countDown();
                    }
                });
            }
            batchDone.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // A poll failure (e.g. a transient database blip) must not kill the scheduled task -
            // Spring stops re-invoking a @Scheduled method that throws.
            log.error("Poll cycle failed for worker {}", properties.workerId(), e);
        }
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        shuttingDown.set(true);
        workerPool.shutdown();
        if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
            log.warn("Worker pool did not drain within 30s; forcing shutdown");
            workerPool.shutdownNow();
        }
    }
}
