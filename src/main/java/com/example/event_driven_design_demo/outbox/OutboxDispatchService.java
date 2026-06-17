package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.Outbox;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class OutboxDispatchService {

    private final OutboxClaimService claimService;
    private final OutboxProcessor processor;
    private final OutboxProperties outboxProperties;

    public OutboxDispatchService(OutboxClaimService claimService,
                                 OutboxProcessor processor,
                                 OutboxProperties outboxProperties) {
        this.claimService = claimService;
        this.processor = processor;
        this.outboxProperties = outboxProperties;
    }

    @Transactional
    public int dispatchOnce() {
        Instant now = Instant.now();
        int batchSize = outboxProperties.getDispatcher().getBatchSize();
        List<Outbox> claimed = claimService.claimPending(batchSize, now);

        for (Outbox outbox : claimed) {
            processor.processOutbox(outbox);
        }
        return claimed.size();
    }
}
