package com.example.event_driven_design_demo.outbox;

import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class OutboxRetryPolicy {

    private static final int MAX_ATTEMPTS_BEFORE_DLQ = 3;

    public boolean shouldMoveToDlq(int attemptsAfterFailure) {
        return attemptsAfterFailure > MAX_ATTEMPTS_BEFORE_DLQ;
    }

    public Instant nextRetryAt(int attemptsAfterFailure) {
        long backoffSeconds = switch (attemptsAfterFailure) {
            case 1 -> 1L;
            case 2 -> 2L;
            case 3 -> 4L;
            default -> throw new IllegalArgumentException("Invalid attempts for retry scheduling: " + attemptsAfterFailure);
        };
        return Instant.now().plusSeconds(backoffSeconds);
    }
}
