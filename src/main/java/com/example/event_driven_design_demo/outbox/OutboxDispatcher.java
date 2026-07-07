package com.example.event_driven_design_demo.outbox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(name = "outbox.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxDispatcher {

    private final OutboxDispatchService dispatchService;

    public OutboxDispatcher(OutboxDispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    @Scheduled(fixedDelayString = "${outbox.dispatcher.fixed-delay-ms:5000}")
    public void dispatchPendingOutboxRows() {

        try {

            int processed = dispatchService.dispatchOnce();
            if (processed > 0) {
                log.debug("Processed {} outbox row(s) in dispatch cycle", processed);
            }
        } catch (Exception ex) {

            log.error("Unexpected error during outbox dispatch cycle", ex);
        }
    }
}
