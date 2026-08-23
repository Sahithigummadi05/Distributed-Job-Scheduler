package com.sahithi.jobscheduler.repository;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.domain.JobStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * All coordination between concurrent worker instances happens through the SQL in this class -
 * the database is the lock, not application memory. {@link #claimBatch} is the load-bearing
 * method: {@code SELECT ... FOR UPDATE SKIP LOCKED} lets N workers race against the same table
 * and each walk away with a disjoint set of rows, with no application-level locking, no leader
 * election, and no distributed lock service - the same pattern production job queues built on
 * Postgres (Oban, pg-boss, GoodJob) use instead of a separate broker.
 */
@Repository
public class JobRepository {

    private static final RowMapper<Job> JOB_ROW_MAPPER = (rs, rowNum) -> new Job(
            UUID.fromString(rs.getString("id")),
            rs.getString("job_type"),
            rs.getString("payload"),
            JobStatus.valueOf(rs.getString("status")),
            rs.getInt("priority"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            toInstant(rs.getTimestamp("next_run_at")),
            rs.getString("locked_by"),
            toInstant(rs.getTimestamp("locked_at")),
            rs.getString("dedupe_key"),
            rs.getString("last_error"),
            toInstant(rs.getTimestamp("created_at")),
            toInstant(rs.getTimestamp("updated_at")));

    private final NamedParameterJdbcTemplate jdbc;

    public JobRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts a new PENDING job. If {@code dedupeKey} is non-null and a job with that key
     * already exists, the insert is a no-op and the existing job's id is returned instead - this
     * is what makes re-submitting the same logical job (e.g. a caller retrying a timed-out HTTP
     * request) safe rather than double-enqueuing it.
     */
    public UUID enqueue(String jobType, String payloadJson, int priority, int maxAttempts, String dedupeKey) {
        var params = new MapSqlParameterSource()
                .addValue("jobType", jobType)
                .addValue("payload", payloadJson)
                .addValue("priority", priority)
                .addValue("maxAttempts", maxAttempts)
                .addValue("dedupeKey", dedupeKey);

        var inserted = jdbc.queryForList(
                """
                INSERT INTO jobs (job_type, payload, priority, max_attempts, dedupe_key)
                VALUES (:jobType, CAST(:payload AS jsonb), :priority, :maxAttempts, :dedupeKey)
                ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
                RETURNING id
                """,
                params,
                UUID.class);

        if (!inserted.isEmpty()) {
            return inserted.get(0);
        }
        if (dedupeKey == null) {
            // Should be unreachable (a NULL dedupe key never conflicts), but fail loudly rather
            // than silently returning nothing if the ON CONFLICT clause's behavior ever changes.
            throw new IllegalStateException("Insert returned no id for a job with no dedupe key");
        }
        return findByDedupeKey(dedupeKey)
                .map(Job::id)
                .orElseThrow(() -> new IllegalStateException(
                        "Dedupe key '" + dedupeKey + "' conflicted but no existing job was found"));
    }

    /**
     * Atomically claims up to {@code batchSize} due jobs for {@code workerId}: the inner SELECT
     * picks candidate rows and skips any another transaction already has locked (SKIP LOCKED),
     * so concurrent callers never block on or double-claim the same row; the outer UPDATE marks
     * exactly those rows RUNNING in the same statement the lock was taken in.
     */
    public List<Job> claimBatch(String workerId, int batchSize) {
        var params = new MapSqlParameterSource()
                .addValue("workerId", workerId)
                .addValue("batchSize", batchSize);

        return jdbc.query(
                """
                WITH claimed AS (
                    SELECT id FROM jobs
                    WHERE status = 'PENDING' AND next_run_at <= now()
                    ORDER BY priority DESC, created_at ASC
                    LIMIT :batchSize
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs
                SET status = 'RUNNING', locked_by = :workerId, locked_at = now(),
                    attempts = attempts + 1, updated_at = now()
                WHERE id IN (SELECT id FROM claimed)
                RETURNING *
                """,
                params,
                JOB_ROW_MAPPER);
    }

    public void markSucceeded(UUID id) {
        jdbc.update(
                "UPDATE jobs SET status = 'SUCCEEDED', updated_at = now() WHERE id = :id",
                new MapSqlParameterSource("id", id));
    }

    public void rescheduleAfterFailure(UUID id, String error, Instant nextRunAt) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("error", error)
                .addValue("nextRunAt", Timestamp.from(nextRunAt));
        jdbc.update(
                """
                UPDATE jobs
                SET status = 'PENDING', next_run_at = :nextRunAt, last_error = :error,
                    locked_by = NULL, locked_at = NULL, updated_at = now()
                WHERE id = :id
                """,
                params);
    }

    public void markDeadLetter(UUID id, String error) {
        jdbc.update(
                """
                UPDATE jobs
                SET status = 'DEAD_LETTER', last_error = :error, updated_at = now()
                WHERE id = :id
                """,
                new MapSqlParameterSource().addValue("id", id).addValue("error", error));
    }

    public Optional<Job> findById(UUID id) {
        var results = jdbc.query(
                "SELECT * FROM jobs WHERE id = :id", new MapSqlParameterSource("id", id), JOB_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public Optional<Job> findByDedupeKey(String dedupeKey) {
        var results = jdbc.query(
                "SELECT * FROM jobs WHERE dedupe_key = :dedupeKey",
                new MapSqlParameterSource("dedupeKey", dedupeKey),
                JOB_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public List<Job> findByStatus(JobStatus status, int limit) {
        var params = new MapSqlParameterSource().addValue("status", status.name()).addValue("limit", limit);
        return jdbc.query(
                "SELECT * FROM jobs WHERE status = :status ORDER BY created_at DESC LIMIT :limit",
                params,
                JOB_ROW_MAPPER);
    }

    public Map<String, Long> countByStatus() {
        return jdbc.query(
                "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status",
                rs -> {
                    var counts = new java.util.LinkedHashMap<String, Long>();
                    while (rs.next()) {
                        counts.put(rs.getString("status"), rs.getLong("cnt"));
                    }
                    return counts;
                });
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
