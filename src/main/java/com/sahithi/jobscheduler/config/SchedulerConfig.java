package com.sahithi.jobscheduler.config;

import com.sahithi.jobscheduler.retry.RetryPolicy;
import java.time.Duration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfig {

    @Bean
    public RetryPolicy retryPolicy(SchedulerProperties properties) {
        return new RetryPolicy(
                Duration.ofSeconds(properties.backoffBaseSeconds()),
                Duration.ofSeconds(properties.backoffMaxSeconds()));
    }
}
