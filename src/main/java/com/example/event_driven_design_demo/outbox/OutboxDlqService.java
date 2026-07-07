package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class OutboxDlqService {

    private static final Logger log = LoggerFactory.getLogger(OutboxDlqService.class);

    private final OutboxRepository outboxRepository;
    private final OutboxDlqRepository outboxDlqRepository;

    public OutboxDlqService(OutboxRepository outboxRepository, OutboxDlqRepository outboxDlqRepository) {
        this.outboxRepository = outboxRepository;
        this.outboxDlqRepository = outboxDlqRepository;
    }

    @Transactional
    public void moveToDlq(OutboxEvent outboxEvent, String failureReason) {
        Instant now = Instant.now();

        OutboxDlq dlq = new OutboxDlq();
        dlq.setOriginalOutboxId(outboxEvent.getId());
        dlq.setApplicationId(outboxEvent.getApplicationId());
        dlq.setCorrelationId(outboxEvent.getCorrelationId());
        dlq.setPayload(outboxEvent.getPayload());
        dlq.setFailureReason(truncate(failureReason, 4000));
        dlq.setAttempts(outboxEvent.getAttempts());
        dlq.setFailedAt(now);
        dlq.setCreatedAt(now);
        outboxDlqRepository.save(dlq);

        outboxEvent.setStatus(OutboxStatus.FAILED.name());
        outboxRepository.save(outboxEvent);

        log.error("Moved outbox row to DLQ outboxId={} dlqId={} applicationId={} correlationId={} attempts={} failureReason={}",
                outboxEvent.getId(), dlq.getId(), outboxEvent.getApplicationId(), outboxEvent.getCorrelationId(),
                outboxEvent.getAttempts(), dlq.getFailureReason());
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
