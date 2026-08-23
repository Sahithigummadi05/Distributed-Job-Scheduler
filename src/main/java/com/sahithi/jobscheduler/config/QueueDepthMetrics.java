package com.sahithi.jobscheduler.config;

import com.sahithi.jobscheduler.domain.JobStatus;
import com.sahithi.jobscheduler.repository.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes queue depth per status as gauges (jobs.queue.depth{status=...}).
 *
 * <p>Backlog is the number you actually alert on: a growing PENDING count means workers aren't
 * keeping up, and any nonzero DEAD_LETTER means work is being dropped and needs a human.
 *
 * <p>The counts are refreshed on a schedule and cached rather than queried inside the gauge
 * callback, because Micrometer polls gauges on the scrape path - a {@code GROUP BY} against the
 * jobs table on every scrape would put the monitoring system in the critical path of the database.
 */
@Component
public class QueueDepthMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueDepthMetrics.class);

    private final JobRepository repository;
    private final MeterRegistry meterRegistry;
    private final AtomicReference<Map<String, Long>> cachedCounts =
            new AtomicReference<>(Map.of());

    public QueueDepthMetrics(JobRepository repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    void registerGauges() {
        for (var status : JobStatus.values()) {
            Gauge.builder("jobs.queue.depth", () -> cachedCounts.get().getOrDefault(status.name(), 0L))
                    .tag("status", status.name())
                    .description("Number of jobs currently in this status")
                    .register(meterRegistry);
        }
    }

    @Scheduled(fixedDelayString = "${scheduler.metrics-refresh-ms:10000}")
    void refresh() {
        try {
            cachedCounts.set(repository.countByStatus());
        } catch (Exception e) {
            // Stale metrics are better than a dead scheduled task - Spring stops re-invoking a
            // @Scheduled method that throws.
            log.warn("Failed to refresh queue depth metrics", e);
        }
    }
}
