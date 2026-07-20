package com.example.event_driven_design_demo.outbox.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.fixtures.OutboxDlqTestDataFactory;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;

/**
 * Unit tests for the {@code orElseThrow} lookup branches. Resilience annotations on these methods are
 * verified in the resilience/integration layer, not here.
 */
@ExtendWith(MockitoExtension.class)
class OutboxReplayReaderTest {

    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private OutboxDlqRepository outboxDlqRepository;
    @Mock
    private ApplicationRepository applicationRepository;

    @InjectMocks
    private OutboxReplayReader reader;

    @Test
    void findOutboxEvent_returnsEntity_whenPresent() {
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        when(outboxRepository.findById(1L)).thenReturn(Optional.of(event));

        assertThat(reader.findOutboxEvent(1L)).isSameAs(event);
    }

    @Test
    void findOutboxEvent_throws_whenAbsent() {
        when(outboxRepository.findById(1L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> reader.findOutboxEvent(1L))
                .isInstanceOf(OutboxReplayException.class)
                .hasMessageContaining("Outbox row not found");
    }

    @Test
    void findDlq_returnsEntity_whenPresent() {
        OutboxDlq dlq = OutboxDlqTestDataFactory.dlq();
        when(outboxDlqRepository.findById(2L)).thenReturn(Optional.of(dlq));

        assertThat(reader.findDlq(2L)).isSameAs(dlq);
    }

    @Test
    void findDlq_throws_whenAbsent() {
        when(outboxDlqRepository.findById(2L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> reader.findDlq(2L))
                .isInstanceOf(OutboxReplayException.class)
                .hasMessageContaining("DLQ row not found");
    }

    @Test
    void findApplication_returnsEntity_whenPresent() {
        UUID id = UUID.randomUUID();
        Application application = ApplicationTestDataFactory.applicationWith(id, UUID.randomUUID());
        when(applicationRepository.findById(id)).thenReturn(Optional.of(application));

        assertThat(reader.findApplication(id)).isSameAs(application);
    }

    @Test
    void findApplication_throws_whenAbsent() {
        UUID id = UUID.randomUUID();
        when(applicationRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> reader.findApplication(id))
                .isInstanceOf(OutboxReplayException.class)
                .hasMessageContaining("Application not found");
    }
}
