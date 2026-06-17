package com.example.event_driven_design_demo.dto;

import java.time.Instant;
import java.util.UUID;

public record OutboxDlqResponse(
        Long id,
        Long originalOutboxId,
        UUID applicationId,
        UUID correlationId,
        String failureReason,
        Integer attempts,
        Instant failedAt,
        Instant createdAt
) {
}
