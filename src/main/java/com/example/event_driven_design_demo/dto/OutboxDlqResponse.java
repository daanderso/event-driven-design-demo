package com.example.event_driven_design_demo.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Dead-letter queue entry for a failed outbox publish attempt")
public record OutboxDlqResponse(
        @Schema(description = "DLQ row identifier", example = "1") Long id,
        @Schema(description = "Original outbox row identifier", example = "42") Long originalOutboxId,
        @Schema(description = "Application identifier associated with the failed event", example = "3fa85f64-5717-4562-b3fc-2c963f66afa6") UUID applicationId,
        @Schema(description = "Correlation identifier for tracing", example = "9b2c4f1e-8d3a-4b5c-9e6f-1234567890ab") UUID correlationId,
        @Schema(description = "Reason the outbox publish ultimately failed", example = "Kafka broker unavailable") String failureReason,
        @Schema(description = "Number of dispatch attempts before DLQ placement", example = "5") Integer attempts,
        @Schema(description = "Timestamp when the event was moved to the DLQ", example = "2026-07-10T17:25:00Z") Instant failedAt,
        @Schema(description = "Timestamp when the DLQ row was created", example = "2026-07-10T17:25:01Z") Instant createdAt
) {
}
