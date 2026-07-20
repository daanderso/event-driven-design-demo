package com.example.event_driven_design_demo.fixtures;

import java.time.Instant;
import java.util.UUID;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.ApplicationStatus;

/**
 * Builds valid and invalid {@link ApplicationRequest} payloads and {@link Application} entities
 * for use across unit, API, and integration tests.
 */
public final class ApplicationTestDataFactory {

    public static final String VALID_FIRST_NAME = "Jane";
    public static final String VALID_LAST_NAME = "Doe";

    private ApplicationTestDataFactory() {
    }

    public static ApplicationRequest validRequest() {
        return requestWith(VALID_FIRST_NAME, VALID_LAST_NAME);
    }

    public static ApplicationRequest requestWith(String firstName, String lastName) {
        ApplicationRequest request = new ApplicationRequest();
        request.setFirstName(firstName);
        request.setLastName(lastName);
        return request;
    }

    public static Application validApplication() {
        return applicationWith(UUID.randomUUID(), UUID.randomUUID());
    }

    public static Application applicationWith(UUID applicationId, UUID correlationId) {
        Instant now = Instant.now();
        Application application = new Application();
        application.setApplicationId(applicationId);
        application.setCorrelationId(correlationId);
        application.setFirstName(VALID_FIRST_NAME);
        application.setLastName(VALID_LAST_NAME);
        application.setStatus(ApplicationStatus.SUBMITTED.name());
        application.setCreatedAt(now);
        application.setUpdatedAt(now);
        return application;
    }

    /** A name exactly at the 50-character boundary (valid). */
    public static String maxLengthName() {
        return "A".repeat(50);
    }

    /** A name one character over the 50-character limit (invalid). */
    public static String tooLongName() {
        return "A".repeat(51);
    }
}
