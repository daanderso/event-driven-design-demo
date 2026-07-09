package com.example.event_driven_design_demo.resilience;

// See docs/06-resilience-design.md for naming convention and rationale.
public final class ResilienceInstanceNames {

    public static final String APPLICATION_SUBMISSION_PERSISTENCE = "application-submission-persistence";

    public static final String OUTBOX_PERSISTENCE = "outbox-persistence";

    private ResilienceInstanceNames() {
    }
}
