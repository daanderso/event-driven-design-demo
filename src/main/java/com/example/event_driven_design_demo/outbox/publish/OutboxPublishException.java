package com.example.event_driven_design_demo.outbox.publish;

public class OutboxPublishException extends RuntimeException {

    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
