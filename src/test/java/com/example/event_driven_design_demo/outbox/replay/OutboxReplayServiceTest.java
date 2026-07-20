package com.example.event_driven_design_demo.outbox.replay;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.fixtures.OutboxDlqTestDataFactory;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.publish.OutboxPublisher;
import com.example.event_driven_design_demo.serialization.ApplicationSubmittedSerializer;

/**
 * Unit tests for the three replay flows and the outbox replay eligibility branch.
 */
@ExtendWith(MockitoExtension.class)
class OutboxReplayServiceTest {

    @Mock
    private OutboxReplayReader replayReader;
    @Mock
    private OutboxPublisher outboxPublisher;
    @Mock
    private ApplicationSubmittedSerializer serializer;

    @InjectMocks
    private OutboxReplayService replayService;

    @Test
    void replayOutbox_whenPublished_republishesPayload() {
        OutboxEvent event = OutboxEventTestDataFactory.published(java.time.Instant.now());
        when(replayReader.findOutboxEvent(1L)).thenReturn(event);

        replayService.replayOutbox(1L);

        verify(outboxPublisher).publishPayload(event.getApplicationId(), event.getCorrelationId(), event.getPayload());
    }

    @Test
    void replayOutbox_whenFailed_republishesPayload() {
        OutboxEvent event = OutboxEventTestDataFactory.failed();
        when(replayReader.findOutboxEvent(2L)).thenReturn(event);

        replayService.replayOutbox(2L);

        verify(outboxPublisher).publishPayload(event.getApplicationId(), event.getCorrelationId(), event.getPayload());
    }

    @Test
    void replayOutbox_whenPending_throwsAndDoesNotPublish() {
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        when(replayReader.findOutboxEvent(3L)).thenReturn(event);

        assertThatThrownBy(() -> replayService.replayOutbox(3L))
                .isInstanceOf(OutboxReplayException.class)
                .hasMessageContaining(OutboxStatus.PENDING.name());
        verifyNoInteractions(outboxPublisher);
    }

    @Test
    void replayDlq_republishesDlqPayload() {
        OutboxDlq dlq = OutboxDlqTestDataFactory.dlq();
        when(replayReader.findDlq(5L)).thenReturn(dlq);

        replayService.replayDlq(5L);

        verify(outboxPublisher).publishPayload(dlq.getApplicationId(), dlq.getCorrelationId(), dlq.getPayload());
    }

    @Test
    void replayFromApplication_reserializesAndRepublishes() {
        UUID applicationId = UUID.randomUUID();
        Application application = ApplicationTestDataFactory.applicationWith(applicationId, UUID.randomUUID());
        byte[] payload = "avro-bytes".getBytes();
        when(replayReader.findApplication(applicationId)).thenReturn(application);
        when(serializer.serialize(application)).thenReturn(payload);

        replayService.replayFromApplication(applicationId);

        verify(serializer).serialize(application);
        verify(outboxPublisher).publishPayload(application.getApplicationId(), application.getCorrelationId(), payload);
    }
}
