package com.sahithi.jobscheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        String workerId,
        long pollIntervalMs,
        int batchSize,
        int executionThreads,
        long backoffBaseSeconds,
        long backoffMaxSeconds,
        /**
         * How long a claim stays valid before the job is considered abandoned. Must be longer
         * than the slowest legitimate job, otherwise a slow job gets reclaimed and run twice
         * while the original attempt is still in flight.
         */
        long leaseTimeoutSeconds) {
}
