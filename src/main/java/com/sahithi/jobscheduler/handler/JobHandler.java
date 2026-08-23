package com.sahithi.jobscheduler.handler;

import com.sahithi.jobscheduler.domain.Job;

/**
 * Implement and register as a Spring bean to handle a job type. Throwing from
 * {@link #handle} marks the attempt failed, which sends the job back through the retry/backoff
 * path (or to the dead-letter queue once attempts are exhausted).
 */
public interface JobHandler {

    /** The {@code job_type} value this handler is responsible for. */
    String jobType();

    void handle(Job job) throws Exception;
}
