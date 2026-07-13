package com.example.event_driven_design_demo.exception;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Standard API error payload returned by the global exception handler")
public class ApiError {

    @Schema(
            description = "Machine-readable error code",
            example = "VALIDATION_ERROR",
            allowableValues = {"VALIDATION_ERROR", "NOT_FOUND", "SERVICE_UNAVAILABLE", "INTERNAL_ERROR"})
    private String errorCode;

    @Schema(description = "Human-readable error message", example = "firstName: must not be blank")
    private String message;

}


