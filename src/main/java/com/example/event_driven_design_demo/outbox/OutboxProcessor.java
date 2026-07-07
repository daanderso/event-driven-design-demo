package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class OutboxProcessor {

    private static final Logger log = LoggerFactory.getLogger(OutboxProcessor.class);

    private final OutboxRepository outboxRepository;
    private final OutboxPublisher outboxPublisher;
    private final OutboxRetryPolicy retryPolicy;
    private final OutboxDlqService dlqService;

    public OutboxProcessor(OutboxRepository outboxRepository,
                           OutboxPublisher outboxPublisher,
                           OutboxRetryPolicy retryPolicy,
                           OutboxDlqService dlqService) {
        this.outboxRepository = outboxRepository;
        this.outboxPublisher = outboxPublisher;
        this.retryPolicy = retryPolicy;
        this.dlqService = dlqService;
    }

    public void processOutbox(OutboxEvent outboxEvent) {
        try {
            outboxPublisher.publish(outboxEvent);
            markPublished(outboxEvent);
        } catch (OutboxPublishException ex) {
            handleFailure(outboxEvent, ex);
        }
    }

    private void markPublished(OutboxEvent outboxEvent) {
        Instant now = Instant.now();
        outboxEvent.setStatus(OutboxStatus.PUBLISHED.name());
        outboxEvent.setPublishedAt(now);
        outboxRepository.save(outboxEvent);
        log.info("Outbox publish succeeded outboxId={} applicationId={} correlationId={} attempt={} status=published",
                outboxEvent.getId(), outboxEvent.getApplicationId(), outboxEvent.getCorrelationId(), outboxEvent.getAttempts());
    }

    private void handleFailure(OutboxEvent outboxEvent, OutboxPublishException ex) {
        int newAttempts = outboxEvent.getAttempts() + 1;
        outboxEvent.setAttempts(newAttempts);
        outboxEvent.setLastError(truncateError(ex));

        if (retryPolicy.shouldMoveToDlq(newAttempts)) {
            dlqService.moveToDlq(outboxEvent, outboxEvent.getLastError());
            return;
        }

        outboxEvent.setScheduledRetryAt(retryPolicy.nextRetryAt(newAttempts));
        outboxRepository.save(outboxEvent);
        log.warn("Outbox publish failed outboxId={} applicationId={} correlationId={} attempt={} status=retry nextRetryAt={} error={}",
                outboxEvent.getId(), outboxEvent.getApplicationId(), outboxEvent.getCorrelationId(),
                newAttempts, outboxEvent.getScheduledRetryAt(), outboxEvent.getLastError());
    }

    private static String truncateError(Throwable ex) {
        Throwable cause = ex.getCause();
        if (cause == null) {
            cause = ex;
        }
        String message = cause.getMessage();
        if (message == null) {
            message = cause.getClass().getSimpleName();
        }
        return message.length() <= 4000 ? message : message.substring(0, 4000);
    }
}
