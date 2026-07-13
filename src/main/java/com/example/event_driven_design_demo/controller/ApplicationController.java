package com.example.event_driven_design_demo.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.event_driven_design_demo.config.StandardServerErrorResponses;
import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.exception.ApiError;
import com.example.event_driven_design_demo.service.ApplicationService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/applications")
@Validated
@Tag(name = "Applications", description = "Submit new applications. Events are written to the outbox and published to Kafka asynchronously.")
public class ApplicationController {

    private final ApplicationService applicationService;

    public ApplicationController(ApplicationService applicationService) {
        this.applicationService = applicationService;
    }

    @Operation(
            summary = "Submit a new application",
            description = """
                    Persists the application and an outbox event in a single transaction.
                    Returns 201 Created with the generated identifiers and status.
                    Kafka publishing is handled by the background outbox processor.""")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "201",
                    description = "Application accepted and persisted",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ApplicationResponse.class))),
            @ApiResponse(
                    responseCode = "400",
                    description = "Request validation failed (blank fields, invalid characters, or length exceeded)",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ApiError.class)))
    })
    @StandardServerErrorResponses
    @PostMapping
    public ResponseEntity<ApplicationResponse> submitApplication(@Valid @RequestBody ApplicationRequest request) {
        log.info("Received application submission for {} {}", request.getFirstName(), request.getLastName());
        ApplicationResponse response = applicationService.submitApplication(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
