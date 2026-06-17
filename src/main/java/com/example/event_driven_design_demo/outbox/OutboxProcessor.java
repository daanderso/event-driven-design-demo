package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.Outbox;
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

    public void processOutbox(Outbox outbox) {
        try {
            outboxPublisher.publish(outbox);
            markPublished(outbox);
        } catch (OutboxPublishException ex) {
            handleFailure(outbox, ex);
        }
    }

    private void markPublished(Outbox outbox) {
        Instant now = Instant.now();
        outbox.setStatus(OutboxStatus.PUBLISHED.name());
        outbox.setPublishedAt(now);
        outboxRepository.save(outbox);
        log.info("Outbox publish succeeded outboxId={} applicationId={} correlationId={} attempt={} status=published",
                outbox.getId(), outbox.getApplicationId(), outbox.getCorrelationId(), outbox.getAttempts());
    }

    private void handleFailure(Outbox outbox, OutboxPublishException ex) {
        int newAttempts = outbox.getAttempts() + 1;
        outbox.setAttempts(newAttempts);
        outbox.setLastError(truncateError(ex));

        if (retryPolicy.shouldMoveToDlq(newAttempts)) {
            dlqService.moveToDlq(outbox, outbox.getLastError());
            return;
        }

        outbox.setScheduledRetryAt(retryPolicy.nextRetryAt(newAttempts));
        outboxRepository.save(outbox);
        log.warn("Outbox publish failed outboxId={} applicationId={} correlationId={} attempt={} status=retry nextRetryAt={} error={}",
                outbox.getId(), outbox.getApplicationId(), outbox.getCorrelationId(),
                newAttempts, outbox.getScheduledRetryAt(), outbox.getLastError());
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
