package com.example.event_driven_design_demo.fixtures;

import java.util.UUID;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.events.ApplicationSubmitted;
import com.example.event_driven_design_demo.serialization.ApplicationSubmittedSerializer;

/**
 * Builds and serializes {@link ApplicationSubmitted} Avro payloads so serializer, publisher,
 * and outbox tests can share consistent binary payloads.
 */
public final class AvroPayloadFactory {

    private static final ApplicationSubmittedSerializer SERIALIZER = new ApplicationSubmittedSerializer();

    private AvroPayloadFactory() {
    }

    public static ApplicationSubmitted sampleEvent() {
        return ApplicationSubmitted.newBuilder()
                .setApplicationId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setTimestamp("2026-07-13T12:00:00Z")
                .setFirstName(ApplicationTestDataFactory.VALID_FIRST_NAME)
                .setLastName(ApplicationTestDataFactory.VALID_LAST_NAME)
                .setVersion(1)
                .build();
    }

    public static ApplicationSubmitted eventFrom(Application application) {
        return ApplicationSubmitted.newBuilder()
                .setApplicationId(application.getApplicationId().toString())
                .setCorrelationId(application.getCorrelationId().toString())
                .setTimestamp(application.getCreatedAt().toString())
                .setFirstName(application.getFirstName())
                .setLastName(application.getLastName())
                .setVersion(1)
                .build();
    }

    public static byte[] serialize(ApplicationSubmitted event) {
        return SERIALIZER.serialize(event);
    }

    /** A reusable, non-empty, valid Avro payload. */
    public static byte[] samplePayload() {
        return serialize(sampleEvent());
    }
}
