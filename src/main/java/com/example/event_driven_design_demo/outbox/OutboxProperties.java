package com.example.event_driven_design_demo.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    private Dispatcher dispatcher = new Dispatcher();
    private Topic topic = new Topic();
    private Retention retention = new Retention();

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public Retention getRetention() {
        return retention;
    }

    public void setRetention(Retention retention) {
        this.retention = retention;
    }

    public static class Dispatcher {
        private boolean enabled = true;
        private long fixedDelayMs = 5000;
        private int batchSize = 50;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getFixedDelayMs() {
            return fixedDelayMs;
        }

        public void setFixedDelayMs(long fixedDelayMs) {
            this.fixedDelayMs = fixedDelayMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    public static class Topic {
        private String applicationSubmitted = "application-submitted";

        public String getApplicationSubmitted() {
            return applicationSubmitted;
        }

        public void setApplicationSubmitted(String applicationSubmitted) {
            this.applicationSubmitted = applicationSubmitted;
        }
    }

    public static class Retention {
        private int publishedDays = 3;
        private int dlqDays = 30;

        public int getPublishedDays() {
            return publishedDays;
        }

        public void setPublishedDays(int publishedDays) {
            this.publishedDays = publishedDays;
        }

        public int getDlqDays() {
            return dlqDays;
        }

        public void setDlqDays(int dlqDays) {
            this.dlqDays = dlqDays;
        }
    }
}
