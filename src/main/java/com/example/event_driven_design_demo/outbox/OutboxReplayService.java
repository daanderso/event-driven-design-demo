package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
public class OutboxReplayService {

    private final OutboxReplayReader replayReader;
    private final OutboxPublisher outboxPublisher;
    private final ApplicationSubmittedSerializer serializer;

    public OutboxReplayService(OutboxReplayReader replayReader,
                               OutboxPublisher outboxPublisher,
                               ApplicationSubmittedSerializer serializer) {
        this.replayReader = replayReader;
        this.outboxPublisher = outboxPublisher;
        this.serializer = serializer;
    }

    public void replayOutbox(Long outboxId) {
        OutboxEvent outboxEvent = replayReader.findOutboxEvent(outboxId);

        String status = outboxEvent.getStatus();
        if (!OutboxStatus.PUBLISHED.name().equals(status) && !OutboxStatus.FAILED.name().equals(status)) {
            throw new OutboxReplayException("Outbox row is not eligible for replay: status=" + status);
        }

        outboxPublisher.publishPayload(outboxEvent.getApplicationId(), outboxEvent.getCorrelationId(), outboxEvent.getPayload());
        log.info("Manual outbox replay succeeded outboxId={} applicationId={} correlationId={}",
                outboxId, outboxEvent.getApplicationId(), outboxEvent.getCorrelationId());
    }

    public void replayDlq(Long dlqId) {
        OutboxDlq dlq = replayReader.findDlq(dlqId);

        outboxPublisher.publishPayload(dlq.getApplicationId(), dlq.getCorrelationId(), dlq.getPayload());
        log.info("Manual DLQ replay succeeded dlqId={} applicationId={} correlationId={} originalOutboxId={}",
                dlqId, dlq.getApplicationId(), dlq.getCorrelationId(), dlq.getOriginalOutboxId());
    }

    public void replayFromApplication(UUID applicationId) {
        Application application = replayReader.findApplication(applicationId);

        byte[] payload = serializer.serialize(application);
        outboxPublisher.publishPayload(application.getApplicationId(), application.getCorrelationId(), payload);
        log.info("Application fallback replay succeeded applicationId={} correlationId={}",
                applicationId, application.getCorrelationId());
    }
}
