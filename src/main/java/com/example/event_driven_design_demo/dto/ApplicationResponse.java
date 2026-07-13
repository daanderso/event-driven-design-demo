package com.example.event_driven_design_demo.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Application submission result")
public class ApplicationResponse {

    @Schema(description = "Unique application identifier", example = "3fa85f64-5717-4562-b3fc-2c963f66afa6")
    private String applicationId;

    @Schema(description = "Correlation identifier for tracing the submission flow", example = "9b2c4f1e-8d3a-4b5c-9e6f-1234567890ab")
    private String correlationId;

    @Schema(description = "Submission timestamp in ISO-8601 UTC", example = "2026-07-10T17:30:00.123456789Z")
    private String timestamp;

    @Schema(description = "Current application status", example = "SUBMITTED")
    private String status;

}


