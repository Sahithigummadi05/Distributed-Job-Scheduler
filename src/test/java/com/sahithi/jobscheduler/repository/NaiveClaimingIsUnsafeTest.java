package com.sahithi.jobscheduler.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

/**
 * Demonstrates *why* {@code FOR UPDATE SKIP LOCKED} is load-bearing, by running the obvious
 * naive alternative - "SELECT the pending rows, then UPDATE them to RUNNING" - under the same
 * concurrency and showing it double-claims jobs.
 *
 * This exists so the passing concurrency test in {@link JobRepositoryConcurrencyTest} means
 * something: a test that would pass with or without the mechanism it's meant to verify proves
 * nothing. Here the naive query is shown to actually break, so the real one passing is evidence
 * that SKIP LOCKED - not luck or insufficient contention - is what makes claiming safe.
 */
@SpringBootTest
@ActiveProfiles("test")
class NaiveClaimingIsUnsafeTest {

    @Autowired
    private JobRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate namedJdbc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearJobs() {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
    }

    /** The tempting-but-wrong version: read candidates, then claim them in a separate statement. */
    private List<UUID> naiveClaimBatch(String workerId, int batchSize) {
        var candidates = namedJdbc.queryForList(
                """
                SELECT id FROM jobs
                WHERE status = 'PENDING' AND next_run_at <= now()
                ORDER BY priority DESC, created_at ASC
                LIMIT :batchSize
                """,
                new MapSqlParameterSource("batchSize", batchSize),
                UUID.class);

        if (candidates.isEmpty()) {
            return List.of();
        }
        // Between the SELECT above and this UPDATE, another worker can read the same rows -
        // there is no lock held across the gap. Both workers then "claim" the same jobs.
        namedJdbc.update(
                "UPDATE jobs SET status = 'RUNNING', locked_by = :workerId, attempts = attempts + 1 WHERE id IN (:ids)",
                new MapSqlParameterSource().addValue("workerId", workerId).addValue("ids", candidates));
        return candidates;
    }

    @Test
    @DisplayName("naive SELECT-then-UPDATE claiming double-processes jobs under concurrency")
    void naiveClaimingDoubleClaimsJobs() throws Exception {
        final var jobCount = 300;
        final var workerCount = 16;

        for (var i = 0; i < jobCount; i++) {
            repository.enqueue("test.job", "{}", 0, 3, null);
        }

        var allClaimed = new ConcurrentLinkedQueue<UUID>();
        var startGate = new CountDownLatch(1);
        var doneGate = new CountDownLatch(workerCount);
        var pool = Executors.newFixedThreadPool(workerCount);

        for (var w = 0; w < workerCount; w++) {
            var workerId = "naive-worker-" + w;
            pool.submit(() -> {
                try {
                    startGate.await();
                    List<UUID> claimed;
                    do {
                        claimed = naiveClaimBatch(workerId, 10);
                        allClaimed.addAll(claimed);
                    } while (!claimed.isEmpty());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(doneGate.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        var distinct = Set.copyOf(allClaimed);
        var duplicateClaims = allClaimed.size() - distinct.size();

        System.out.printf(
                "Naive claiming: %d total claims for %d distinct jobs -> %d duplicate claims%n",
                allClaimed.size(), distinct.size(), duplicateClaims);

        // This is the bug being demonstrated: at least one job got handed to more than one
        // worker. (Asserting > 0 rather than an exact count - the amount of overlap depends on
        // scheduling, but under this much contention some overlap is reliably produced.)
        assertThat(duplicateClaims)
                .as("naive claiming should double-claim at least one job under contention")
                .isGreaterThan(0);
    }
}
