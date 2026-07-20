package com.example.event_driven_design_demo.fixtures;

import java.time.Instant;
import java.util.UUID;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;

/**
 * Builds {@link OutboxEvent} rows in each lifecycle status with configurable attempts and timestamps.
 */
public final class OutboxEventTestDataFactory {

    private OutboxEventTestDataFactory() {
    }

    public static OutboxEvent pending() {
        return pending(0, Instant.now());
    }

    public static OutboxEvent pending(int attempts, Instant scheduledRetryAt) {
        OutboxEvent event = baseEvent();
        event.setStatus(OutboxStatus.PENDING.name());
        event.setAttempts(attempts);
        event.setScheduledRetryAt(scheduledRetryAt);
        return event;
    }

    public static OutboxEvent published(Instant publishedAt) {
        OutboxEvent event = baseEvent();
        event.setStatus(OutboxStatus.PUBLISHED.name());
        event.setAttempts(1);
        event.setScheduledRetryAt(publishedAt);
        event.setPublishedAt(publishedAt);
        return event;
    }

    public static OutboxEvent failed() {
        OutboxEvent event = baseEvent();
        event.setStatus(OutboxStatus.FAILED.name());
        event.setAttempts(4);
        event.setScheduledRetryAt(Instant.now());
        return event;
    }

    private static OutboxEvent baseEvent() {
        Instant now = Instant.now();
        OutboxEvent event = new OutboxEvent();
        event.setApplicationId(UUID.randomUUID());
        event.setCorrelationId(UUID.randomUUID());
        event.setPayload(AvroPayloadFactory.samplePayload());
        event.setContentType("avro/binary");
        event.setAttempts(0);
        event.setCreatedAt(now);
        event.setScheduledRetryAt(now);
        return event;
    }
}
