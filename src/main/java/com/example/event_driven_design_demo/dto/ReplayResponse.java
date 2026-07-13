package com.example.event_driven_design_demo.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Result of a manual outbox or DLQ replay operation")
public record ReplayResponse(
        @Schema(description = "Human-readable replay outcome message", example = "DLQ replay published to Kafka") String message,
        @Schema(description = "Whether the replay publish succeeded", example = "true") boolean success
) {
}
