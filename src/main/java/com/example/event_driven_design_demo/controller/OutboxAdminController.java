package com.example.event_driven_design_demo.controller;

import com.example.event_driven_design_demo.config.StandardServerErrorResponses;
import com.example.event_driven_design_demo.dto.OutboxDlqResponse;
import com.example.event_driven_design_demo.dto.ReplayResponse;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.exception.ApiError;
import com.example.event_driven_design_demo.outbox.replay.OutboxReplayException;
import com.example.event_driven_design_demo.outbox.replay.OutboxReplayService;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
@Tag(name = "Outbox Admin", description = "Operator endpoints for inspecting dead-letter queue items and triggering manual replays.")
public class OutboxAdminController {

    private final OutboxDlqRepository outboxDlqRepository;
    private final OutboxReplayService replayService;

    public OutboxAdminController(OutboxDlqRepository outboxDlqRepository, OutboxReplayService replayService) {
        this.outboxDlqRepository = outboxDlqRepository;
        this.replayService = replayService;
    }

    @Operation(
            summary = "List DLQ items",
            description = "Returns a paginated list of outbox dead-letter queue entries. Supports standard Spring Data pagination query parameters (`page`, `size`, `sort`).")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Paginated DLQ items",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = OutboxDlqResponse.class)))
    })
    @StandardServerErrorResponses
    @GetMapping("/outbox-dlq")
    public Page<OutboxDlqResponse> listDlqItems(@ParameterObject Pageable pageable) {
        return outboxDlqRepository.findAll(pageable).map(this::toResponse);
    }

    @Operation(
            summary = "Get DLQ item by id",
            description = "Returns full details for a single dead-letter queue entry.")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "DLQ item found",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = OutboxDlqResponse.class))),
            @ApiResponse(
                    responseCode = "404",
                    description = "DLQ row not found",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ApiError.class)))
    })
    @StandardServerErrorResponses
    @GetMapping("/outbox-dlq/{id}")
    public OutboxDlqResponse getDlqItem(
            @Parameter(description = "DLQ row identifier", example = "1") @PathVariable Long id) {
        OutboxDlq dlq = outboxDlqRepository.findById(id)
                .orElseThrow(() -> new OutboxReplayException("DLQ row not found: " + id));
        return toResponse(dlq);
    }

    @Operation(
            summary = "Replay a DLQ item",
            description = "Republishes the stored payload from a DLQ entry directly to Kafka.")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "DLQ payload successfully published to Kafka",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ReplayResponse.class))),
            @ApiResponse(
                    responseCode = "404",
                    description = "DLQ row not found",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ApiError.class)))
    })
    @StandardServerErrorResponses
    @PostMapping("/outbox-dlq/{id}/replay")
    public ResponseEntity<ReplayResponse> replayDlq(
            @Parameter(description = "DLQ row identifier", example = "1") @PathVariable Long id) {
        replayService.replayDlq(id);
        return ResponseEntity.ok(new ReplayResponse("DLQ replay published to Kafka", true));
    }

    @Operation(
            summary = "Replay an outbox row",
            description = """
                    Republishes the payload from an outbox row directly to Kafka.
                    The outbox row must be in PUBLISHED or FAILED status; otherwise a 404 is returned.""")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Outbox payload successfully published to Kafka",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ReplayResponse.class))),
            @ApiResponse(
                    responseCode = "404",
                    description = "Outbox row not found or not eligible for replay",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ApiError.class)))
    })
    @StandardServerErrorResponses
    @PostMapping("/outbox/{id}/replay")
    public ResponseEntity<ReplayResponse> replayOutbox(
            @Parameter(description = "Outbox row identifier", example = "42") @PathVariable Long id) {
        replayService.replayOutbox(id);
        return ResponseEntity.ok(new ReplayResponse("Outbox replay published to Kafka", true));
    }

    private OutboxDlqResponse toResponse(OutboxDlq dlq) {
        return new OutboxDlqResponse(
                dlq.getId(),
                dlq.getOriginalOutboxId(),
                dlq.getApplicationId(),
                dlq.getCorrelationId(),
                dlq.getFailureReason(),
                dlq.getAttempts(),
                dlq.getFailedAt(),
                dlq.getCreatedAt());
    }
}
