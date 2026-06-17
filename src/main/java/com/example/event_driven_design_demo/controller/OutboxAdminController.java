package com.example.event_driven_design_demo.controller;

import com.example.event_driven_design_demo.dto.OutboxDlqResponse;
import com.example.event_driven_design_demo.dto.ReplayResponse;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.outbox.OutboxReplayException;
import com.example.event_driven_design_demo.outbox.OutboxReplayService;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
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
public class OutboxAdminController {

    private final OutboxDlqRepository outboxDlqRepository;
    private final OutboxReplayService replayService;

    public OutboxAdminController(OutboxDlqRepository outboxDlqRepository, OutboxReplayService replayService) {
        this.outboxDlqRepository = outboxDlqRepository;
        this.replayService = replayService;
    }

    @GetMapping("/outbox-dlq")
    public Page<OutboxDlqResponse> listDlqItems(Pageable pageable) {
        return outboxDlqRepository.findAll(pageable).map(this::toResponse);
    }

    @GetMapping("/outbox-dlq/{id}")
    public OutboxDlqResponse getDlqItem(@PathVariable Long id) {
        OutboxDlq dlq = outboxDlqRepository.findById(id)
                .orElseThrow(() -> new OutboxReplayException("DLQ row not found: " + id));
        return toResponse(dlq);
    }

    @PostMapping("/outbox-dlq/{id}/replay")
    public ResponseEntity<ReplayResponse> replayDlq(@PathVariable Long id) {
        replayService.replayDlq(id);
        return ResponseEntity.ok(new ReplayResponse("DLQ replay published to Kafka", true));
    }

    @PostMapping("/outbox/{id}/replay")
    public ResponseEntity<ReplayResponse> replayOutbox(@PathVariable Long id) {
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
