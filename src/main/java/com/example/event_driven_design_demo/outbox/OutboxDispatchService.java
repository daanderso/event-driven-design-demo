package com.example.event_driven_design_demo.outbox;

import java.time.Instant;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.event_driven_design_demo.entity.OutboxEvent;

@Service
public class OutboxDispatchService {

    private final OutboxRetrievalService outboxRetrievalService;
    private final OutboxProcessor processor;
    private final OutboxProperties outboxProperties;

    public OutboxDispatchService(OutboxRetrievalService outboxRetrievalService,
            OutboxProcessor processor,
            OutboxProperties outboxProperties) {
        this.outboxRetrievalService = outboxRetrievalService;
        this.processor = processor;
        this.outboxProperties = outboxProperties;
    }

    @Transactional
    public int dispatchOnce() {

        Instant now = Instant.now();
        int batchSize = outboxProperties.getDispatcher().getBatchSize();
        List<OutboxEvent> pendingOutboxEvents =

                outboxRetrievalService.retrievePendingOutboxEvents(batchSize, now);
        for (OutboxEvent outboxEvent : pendingOutboxEvents) {

            processor.processOutbox(outboxEvent);

        }
        return pendingOutboxEvents.size();
    }

}
