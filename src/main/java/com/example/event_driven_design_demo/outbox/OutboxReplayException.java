package com.example.event_driven_design_demo.outbox;

public class OutboxReplayException extends RuntimeException {

    public OutboxReplayException(String message) {
        super(message);
    }
}
