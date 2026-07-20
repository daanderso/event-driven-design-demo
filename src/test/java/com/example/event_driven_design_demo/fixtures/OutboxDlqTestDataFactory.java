package com.example.event_driven_design_demo.fixtures;

import java.time.Instant;
import java.util.UUID;

import com.example.event_driven_design_demo.entity.OutboxDlq;

/**
 * Builds {@link OutboxDlq} rows with configurable failure timestamp for cleanup/replay tests.
 */
public final class OutboxDlqTestDataFactory {

    private OutboxDlqTestDataFactory() {
    }

    public static OutboxDlq dlq() {
        return dlqFailedAt(Instant.now());
    }

    public static OutboxDlq dlqFailedAt(Instant failedAt) {
        OutboxDlq dlq = new OutboxDlq();
        dlq.setOriginalOutboxId(42L);
        dlq.setApplicationId(UUID.randomUUID());
        dlq.setCorrelationId(UUID.randomUUID());
        dlq.setPayload(AvroPayloadFactory.samplePayload());
        dlq.setFailureReason("Simulated permanent failure");
        dlq.setAttempts(4);
        dlq.setFailedAt(failedAt);
        dlq.setCreatedAt(failedAt);
        return dlq;
    }
}
