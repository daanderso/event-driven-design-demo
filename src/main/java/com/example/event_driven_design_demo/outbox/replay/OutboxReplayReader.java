package com.example.event_driven_design_demo.outbox.replay;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.resilience.ResilienceInstanceNames;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.stereotype.Service;

import java.util.UUID;

// See docs/06-resilience-design.md for why replay DB reads are isolated in this bean.
@Service
public class OutboxReplayReader {

    private final OutboxRepository outboxRepository;
    private final OutboxDlqRepository outboxDlqRepository;
    private final ApplicationRepository applicationRepository;

    public OutboxReplayReader(OutboxRepository outboxRepository,
                              OutboxDlqRepository outboxDlqRepository,
                              ApplicationRepository applicationRepository) {
        this.outboxRepository = outboxRepository;
        this.outboxDlqRepository = outboxDlqRepository;
        this.applicationRepository = applicationRepository;
    }

    @Retry(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
    @CircuitBreaker(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
    public OutboxEvent findOutboxEvent(Long outboxId) {
        return outboxRepository.findById(outboxId)
                .orElseThrow(() -> new OutboxReplayException("Outbox row not found: " + outboxId));
    }

    @Retry(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
    @CircuitBreaker(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
    public OutboxDlq findDlq(Long dlqId) {
        return outboxDlqRepository.findById(dlqId)
                .orElseThrow(() -> new OutboxReplayException("DLQ row not found: " + dlqId));
    }

    @Retry(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
    @CircuitBreaker(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
    public Application findApplication(UUID applicationId) {
        return applicationRepository.findById(applicationId)
                .orElseThrow(() -> new OutboxReplayException("Application not found: " + applicationId));
    }
}
