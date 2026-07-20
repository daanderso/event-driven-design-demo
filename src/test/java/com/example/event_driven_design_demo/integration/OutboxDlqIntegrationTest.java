package com.example.event_driven_design_demo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.outbox.dispatch.OutboxDispatchService;
import com.example.event_driven_design_demo.outbox.publish.OutboxPublishException;
import com.example.event_driven_design_demo.outbox.publish.OutboxPublisher;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.service.ApplicationService;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Validates the outbox publish-failure retry/DLQ path end-to-end against H2. A {@code @MockitoBean}
 * publisher always fails, so the row walks the backoff schedule and is moved to the DLQ after the
 * 4th failed attempt. To avoid waiting on the real 1s/2s/4s backoff, each cycle reloads the row and
 * resets {@code scheduledRetryAt} to now before invoking {@link OutboxDispatchService#dispatchOnce()}.
 */
@SpringBootTest
@ActiveProfiles("test")
class OutboxDlqIntegrationTest {

    @Autowired
    private ApplicationService applicationService;
    @Autowired
    private OutboxDispatchService dispatchService;
    @Autowired
    private OutboxRepository outboxRepository;
    @Autowired
    private OutboxDlqRepository outboxDlqRepository;
    @Autowired
    private ApplicationRepository applicationRepository;
    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @MockitoBean
    private OutboxPublisher outboxPublisher;

    @BeforeEach
    void cleanState() {
        // FK-safe delete order: outbox -> outbox_dlq -> applications.
        outboxRepository.deleteAll();
        outboxDlqRepository.deleteAll();
        applicationRepository.deleteAll();
        circuitBreakerRegistry.circuitBreaker("outbox-persistence").reset();

        doThrow(new OutboxPublishException("boom", new RuntimeException("broker down")))
                .when(outboxPublisher).publish(any());
    }

    @Test
    void repeatedPublishFailures_moveRowToDlqAfterFourthAttempt() {
        UUID applicationId = submitApplication();

        for (int cycle = 0; cycle < 4; cycle++) {
            OutboxEvent row = outboxRowFor(applicationId);
            row.setScheduledRetryAt(Instant.now());
            outboxRepository.save(row);
            dispatchService.dispatchOnce();
        }

        OutboxEvent source = outboxRowFor(applicationId);
        assertThat(source.getStatus()).isEqualTo(OutboxStatus.FAILED.name());
        assertThat(source.getAttempts()).isEqualTo(4);

        List<OutboxDlq> dlqRows = outboxDlqRepository.findAll().stream()
                .filter(d -> applicationId.equals(d.getApplicationId()))
                .toList();
        assertThat(dlqRows).hasSize(1);
        assertThat(dlqRows.getFirst().getAttempts()).isEqualTo(4);
    }

    @Test
    void singlePublishFailure_reschedulesRowForRetry() {
        UUID applicationId = submitApplication();

        OutboxEvent row = outboxRowFor(applicationId);
        row.setScheduledRetryAt(Instant.now());
        outboxRepository.save(row);
        Instant beforeDispatch = Instant.now();

        dispatchService.dispatchOnce();

        OutboxEvent afterFailure = outboxRowFor(applicationId);
        assertThat(afterFailure.getStatus()).isEqualTo(OutboxStatus.PENDING.name());
        assertThat(afterFailure.getAttempts()).isEqualTo(1);
        assertThat(afterFailure.getScheduledRetryAt()).isAfter(beforeDispatch);
        assertThat(outboxDlqRepository.count()).isZero();
    }

    private UUID submitApplication() {
        ApplicationResponse response = applicationService.submitApplication(ApplicationTestDataFactory.validRequest());
        return UUID.fromString(response.getApplicationId());
    }

    private OutboxEvent outboxRowFor(UUID applicationId) {
        return outboxRepository.findAll().stream()
                .filter(o -> applicationId.equals(o.getApplicationId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No outbox row for application " + applicationId));
    }
}
