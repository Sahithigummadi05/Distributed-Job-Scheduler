package com.sahithi.jobscheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        String workerId,
        long pollIntervalMs,
        int batchSize,
        int executionThreads,
        long backoffBaseSeconds,
        long backoffMaxSeconds) {
}
