package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.Outbox;
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
    public void moveToDlq(Outbox outbox, String failureReason) {
        Instant now = Instant.now();

        OutboxDlq dlq = new OutboxDlq();
        dlq.setOriginalOutboxId(outbox.getId());
        dlq.setApplicationId(outbox.getApplicationId());
        dlq.setCorrelationId(outbox.getCorrelationId());
        dlq.setPayload(outbox.getPayload());
        dlq.setFailureReason(truncate(failureReason, 4000));
        dlq.setAttempts(outbox.getAttempts());
        dlq.setFailedAt(now);
        dlq.setCreatedAt(now);
        outboxDlqRepository.save(dlq);

        outbox.setStatus(OutboxStatus.FAILED.name());
        outboxRepository.save(outbox);

        log.error("Moved outbox row to DLQ outboxId={} dlqId={} applicationId={} correlationId={} attempts={} failureReason={}",
                outbox.getId(), dlq.getId(), outbox.getApplicationId(), outbox.getCorrelationId(),
                outbox.getAttempts(), dlq.getFailureReason());
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
