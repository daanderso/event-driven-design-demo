package com.example.event_driven_design_demo.outbox.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.outbox.config.OutboxProperties;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;

/**
 * Unit tests for retention cutoff calculation and delete orchestration.
 */
@ExtendWith(MockitoExtension.class)
class OutboxCleanupJobTest {

    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private OutboxDlqRepository outboxDlqRepository;

    private OutboxCleanupJob cleanupJob;

    @BeforeEach
    void setUp() {
        OutboxProperties properties = new OutboxProperties();
        properties.getRetention().setPublishedDays(3);
        properties.getRetention().setDlqDays(30);
        cleanupJob = new OutboxCleanupJob(outboxRepository, outboxDlqRepository, properties);
    }

    @Test
    void purgeExpiredRows_deletesUsingConfiguredRetentionCutoffs() {
        when(outboxRepository.deletePublishedBefore(org.mockito.ArgumentMatchers.any())).thenReturn(2);
        when(outboxDlqRepository.deleteFailedBefore(org.mockito.ArgumentMatchers.any())).thenReturn(1);
        Instant testStart = Instant.now();

        cleanupJob.purgeExpiredRows();

        ArgumentCaptor<Instant> publishedCutoff = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> dlqCutoff = ArgumentCaptor.forClass(Instant.class);
        verify(outboxRepository).deletePublishedBefore(publishedCutoff.capture());
        verify(outboxDlqRepository).deleteFailedBefore(dlqCutoff.capture());

        assertThat(publishedCutoff.getValue())
                .isBetween(testStart.minus(3, ChronoUnit.DAYS).minusSeconds(5),
                        testStart.minus(3, ChronoUnit.DAYS).plusSeconds(5));
        assertThat(dlqCutoff.getValue())
                .isBetween(testStart.minus(30, ChronoUnit.DAYS).minusSeconds(5),
                        testStart.minus(30, ChronoUnit.DAYS).plusSeconds(5));
    }
}
