package com.example.event_driven_design_demo.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties(prefix = "outbox")
@Getter
@Setter
public class OutboxProperties {

    private Dispatcher dispatcher = new Dispatcher();
    private Topic topic = new Topic();
    private Retention retention = new Retention();

    @Getter
    @Setter
    public static class Dispatcher {
        private boolean enabled = true;
        private long fixedDelayMs = 5000;
        private int batchSize = 50;
    }

    @Getter
    @Setter
    public static class Topic {
        private String applicationSubmitted = "application-submitted";
    }

    @Getter
    @Setter
    public static class Retention {
        private int publishedDays = 3;
        private int dlqDays = 30;
    }
}
