package com.sahithi.jobscheduler.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable snapshot of a jobs row. Mutations happen in the database (via
 * {@code JobRepository}'s update statements, not by mutating this object) since the whole point
 * of the design is that the database - not application memory - is the single source of truth
 * multiple worker instances coordinate through.
 */
public record Job(
        UUID id,
        String jobType,
        String payload,
        JobStatus status,
        int priority,
        int attempts,
        int maxAttempts,
        Instant nextRunAt,
        String lockedBy,
        Instant lockedAt,
        String dedupeKey,
        String lastError,
        Instant createdAt,
        Instant updatedAt) {

    public boolean hasAttemptsRemaining() {
        return attempts < maxAttempts;
    }
}
