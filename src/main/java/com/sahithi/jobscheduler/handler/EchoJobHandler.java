package com.sahithi.jobscheduler.handler;

import com.sahithi.jobscheduler.domain.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Reference implementation and smoke-test handler: logs the payload and succeeds. Handy for
 * confirming a deployment is actually draining the queue without wiring up real work first.
 */
@Component
public class EchoJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(EchoJobHandler.class);

    @Override
    public String jobType() {
        return "echo";
    }

    @Override
    public void handle(Job job) {
        log.info("echo job {} payload={}", job.id(), job.payload());
    }
}
