package com.example.event_driven_design_demo.outbox.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;

/**
 * Unit tests for the terminal DLQ transition: copy the event into a DLQ row and mark the source FAILED.
 */
@ExtendWith(MockitoExtension.class)
class OutboxDlqServiceTest {

    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private OutboxDlqRepository outboxDlqRepository;

    @InjectMocks
    private OutboxDlqService dlqService;

    @Test
    void moveToDlq_copiesEventFieldsIntoDlqRow_andMarksSourceFailed() {
        OutboxEvent event = OutboxEventTestDataFactory.pending(4, java.time.Instant.now());
        event.setId(99L);

        dlqService.moveToDlq(event, "kafka broker unavailable");

        ArgumentCaptor<OutboxDlq> dlqCaptor = ArgumentCaptor.forClass(OutboxDlq.class);
        verify(outboxDlqRepository).save(dlqCaptor.capture());
        OutboxDlq dlq = dlqCaptor.getValue();
        assertThat(dlq.getOriginalOutboxId()).isEqualTo(99L);
        assertThat(dlq.getApplicationId()).isEqualTo(event.getApplicationId());
        assertThat(dlq.getCorrelationId()).isEqualTo(event.getCorrelationId());
        assertThat(dlq.getPayload()).isEqualTo(event.getPayload());
        assertThat(dlq.getAttempts()).isEqualTo(event.getAttempts());
        assertThat(dlq.getFailureReason()).isEqualTo("kafka broker unavailable");
        assertThat(dlq.getFailedAt()).isNotNull();
        assertThat(dlq.getCreatedAt()).isNotNull();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED.name());
        verify(outboxRepository).save(event);
    }

    @Test
    void moveToDlq_truncatesFailureReasonTo4000Chars() {
        OutboxEvent event = OutboxEventTestDataFactory.pending(4, java.time.Instant.now());
        String longReason = "y".repeat(5000);

        dlqService.moveToDlq(event, longReason);

        ArgumentCaptor<OutboxDlq> dlqCaptor = ArgumentCaptor.forClass(OutboxDlq.class);
        verify(outboxDlqRepository).save(dlqCaptor.capture());
        assertThat(dlqCaptor.getValue().getFailureReason()).hasSize(4000);
    }
}
