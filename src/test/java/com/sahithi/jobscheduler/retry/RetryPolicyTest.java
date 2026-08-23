package com.sahithi.jobscheduler.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(2), Duration.ofSeconds(300));

    @ParameterizedTest
    @CsvSource({
        "1, 2", // base * 2^0
        "2, 4", // base * 2^1
        "3, 8",
        "4, 16",
        "8, 256",
    })
    void delayStaysWithinTheExponentialCeilingForTheAttempt(int attempt, long expectedCeilingSeconds) {
        // Full jitter means the delay is random in [0, ceiling] - so the property to assert is
        // the bound, not an exact value. Sampled repeatedly to make a bad bound very likely to show.
        for (var i = 0; i < 200; i++) {
            var delay = policy.nextDelay(attempt);
            assertThat(delay).isBetween(Duration.ZERO, Duration.ofSeconds(expectedCeilingSeconds));
        }
    }

    @Test
    void delayIsCappedSoAJobThatKeepsFailingIsNeverScheduledAbsurdlyFarOut() {
        // Without the cap, attempt 30 would be 2s * 2^29 ~= 34 years.
        for (var i = 0; i < 200; i++) {
            assertThat(policy.nextDelay(30)).isLessThanOrEqualTo(Duration.ofSeconds(300));
        }
    }

    @Test
    void veryLargeAttemptNumbersDoNotOverflowIntoNegativeDelays() {
        // 1L << 63 overflows to a negative long; the shift is clamped to guard against that.
        for (var attempt : new int[] {31, 64, 1000, Integer.MAX_VALUE}) {
            var delay = policy.nextDelay(attempt);
            assertThat(delay).isBetween(Duration.ZERO, Duration.ofSeconds(300));
        }
    }

    @Test
    void jitterActuallyVariesRatherThanReturningAConstant() {
        // The point of jitter is to stop failed jobs from retrying in lockstep ("thundering
        // herd"), so a policy that always returned the ceiling would defeat it.
        var distinct = new java.util.HashSet<Duration>();
        for (var i = 0; i < 100; i++) {
            distinct.add(policy.nextDelay(5));
        }
        assertThat(distinct).hasSizeGreaterThan(1);
    }

    @Test
    void rejectsInvalidAttemptNumbers() {
        assertThatThrownBy(() -> policy.nextDelay(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy.nextDelay(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new RetryPolicy(Duration.ZERO, Duration.ofSeconds(10)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(10), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
