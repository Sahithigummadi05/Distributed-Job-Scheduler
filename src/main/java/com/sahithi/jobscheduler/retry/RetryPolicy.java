package com.sahithi.jobscheduler.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter (the AWS-architecture-blog algorithm:
 * {@code random(0, min(cap, base * 2^attempt))}), capped so a job that keeps failing doesn't end
 * up scheduled days in the future.
 *
 * A pure function on purpose - no Spring, no clock, no I/O - so its edge cases (attempt 0, the
 * cap kicking in, jitter bounds) are plain JUnit tests instead of integration tests.
 */
public class RetryPolicy {

    private final Duration base;
    private final Duration cap;

    public RetryPolicy(Duration base, Duration cap) {
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base backoff must be positive");
        }
        if (cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("cap must be >= base");
        }
        this.base = base;
        this.cap = cap;
    }

    /**
     * @param attempt the attempt number that just failed (1 for the first failure)
     * @return how long to wait before the next attempt, uniformly random between zero and the
     *     exponential backoff ceiling for this attempt
     */
    public Duration nextDelay(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
        }

        var exponential = base.multipliedBy(1L << Math.min(attempt - 1, 30));
        var ceiling = exponential.compareTo(cap) > 0 ? cap : exponential;

        var ceilingMillis = ceiling.toMillis();
        if (ceilingMillis <= 0) {
            return Duration.ZERO;
        }
        var jitteredMillis = ThreadLocalRandom.current().nextLong(0, ceilingMillis + 1);
        return Duration.ofMillis(jitteredMillis);
    }
}
