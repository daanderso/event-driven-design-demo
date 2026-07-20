package com.example.event_driven_design_demo.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.config.OutboxProperties;

/**
 * Unit tests for batch orchestration: reads the configured batch size, delegates each row to the
 * processor, and returns the processed count.
 */
@ExtendWith(MockitoExtension.class)
class OutboxDispatchServiceTest {

    @Mock
    private OutboxRetrievalService retrievalService;
    @Mock
    private OutboxProcessor processor;

    private OutboxProperties outboxProperties;
    private OutboxDispatchService dispatchService;

    @BeforeEach
    void setUp() {
        outboxProperties = new OutboxProperties();
        outboxProperties.getDispatcher().setBatchSize(25);
        dispatchService = new OutboxDispatchService(retrievalService, processor, outboxProperties);
    }

    @Test
    void dispatchOnce_processesEachRetrievedRow_andReturnsCount() {
        OutboxEvent first = OutboxEventTestDataFactory.pending();
        OutboxEvent second = OutboxEventTestDataFactory.pending();
        when(retrievalService.retrievePendingOutboxEvents(eq(25), org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(List.of(first, second));

        int processed = dispatchService.dispatchOnce();

        assertThat(processed).isEqualTo(2);
        verify(processor).processOutbox(first);
        verify(processor).processOutbox(second);
    }

    @Test
    void dispatchOnce_whenNothingPending_returnsZeroAndDoesNotProcess() {
        when(retrievalService.retrievePendingOutboxEvents(eq(25), org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(List.of());

        int processed = dispatchService.dispatchOnce();

        assertThat(processed).isZero();
        verifyNoInteractions(processor);
    }
}
