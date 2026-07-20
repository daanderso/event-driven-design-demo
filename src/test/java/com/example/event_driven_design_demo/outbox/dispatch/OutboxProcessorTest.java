package com.example.event_driven_design_demo.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.dlq.OutboxDlqService;
import com.example.event_driven_design_demo.outbox.publish.OutboxPublishException;
import com.example.event_driven_design_demo.outbox.publish.OutboxPublisher;
import com.example.event_driven_design_demo.repository.OutboxRepository;

/**
 * Unit tests for the core outbox failure-handling branching: success, transient retry, and DLQ.
 */
@ExtendWith(MockitoExtension.class)
class OutboxProcessorTest {

    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private OutboxPublisher outboxPublisher;
    @Mock
    private OutboxDlqService dlqService;

    // A real policy keeps the retry/DLQ boundary consistent with production behavior.
    private final OutboxRetryPolicy retryPolicy = new OutboxRetryPolicy();

    private OutboxProcessor processor;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        processor = new OutboxProcessor(outboxRepository, outboxPublisher, retryPolicy, dlqService);
    }

    @Test
    void processOutbox_onSuccessfulPublish_marksPublishedAndSaves() {
        OutboxEvent event = OutboxEventTestDataFactory.pending();

        processor.processOutbox(event);

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED.name());
        assertThat(event.getPublishedAt()).isNotNull();
        verify(outboxPublisher).publish(event);
        verify(outboxRepository).save(event);
        verifyNoInteractions(dlqService);
    }

    @Test
    void processOutbox_onTransientFailure_incrementsAttemptsAndReschedules() {
        OutboxEvent event = OutboxEventTestDataFactory.pending(0, Instant.now());
        doThrow(new OutboxPublishException("boom", new RuntimeException("broker down")))
                .when(outboxPublisher).publish(event);

        processor.processOutbox(event);

        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getLastError()).isEqualTo("broker down");
        assertThat(event.getScheduledRetryAt()).isAfter(Instant.now());
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING.name());
        verify(outboxRepository).save(event);
        verify(dlqService, never()).moveToDlq(any(), anyString());
    }

    @Test
    void processOutbox_whenRetriesExhausted_movesToDlqAndDoesNotSaveOutbox() {
        // Already failed 3 times; the 4th failure exceeds the retry budget -> DLQ.
        OutboxEvent event = OutboxEventTestDataFactory.pending(3, Instant.now());
        doThrow(new OutboxPublishException("boom", new RuntimeException("still down")))
                .when(outboxPublisher).publish(event);

        processor.processOutbox(event);

        assertThat(event.getAttempts()).isEqualTo(4);
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(dlqService).moveToDlq(any(OutboxEvent.class), reason.capture());
        assertThat(reason.getValue()).isEqualTo("still down");
        verify(outboxRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void processOutbox_whenCauseMessageIsNull_usesCauseClassNameAsError() {
        OutboxEvent event = OutboxEventTestDataFactory.pending(0, Instant.now());
        doThrow(new OutboxPublishException("wrapper", new IllegalStateException()))
                .when(outboxPublisher).publish(event);

        processor.processOutbox(event);

        assertThat(event.getLastError()).isEqualTo("IllegalStateException");
    }

    @Test
    void processOutbox_truncatesErrorMessageTo4000Chars() {
        OutboxEvent event = OutboxEventTestDataFactory.pending(0, Instant.now());
        String longMessage = "x".repeat(5000);
        doThrow(new OutboxPublishException("wrapper", new RuntimeException(longMessage)))
                .when(outboxPublisher).publish(event);

        processor.processOutbox(event);

        assertThat(event.getLastError()).hasSize(4000);
    }
}
