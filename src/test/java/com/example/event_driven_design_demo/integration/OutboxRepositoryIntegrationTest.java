package com.example.event_driven_design_demo.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.fixtures.OutboxDlqTestDataFactory;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.cleanup.OutboxCleanupJob;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;

/**
 * Exercises the custom outbox repository queries and the retention cleanup job against a real (H2)
 * database. GOTCHA #1: the H2 schema enforces {@code fk_outbox_app}, so a matching {@link Application}
 * row is persisted before each outbox row that carries the same {@code applicationId}.
 */
@SpringBootTest
@ActiveProfiles("test")
class OutboxRepositoryIntegrationTest {

    @Autowired
    private OutboxRepository outboxRepository;
    @Autowired
    private OutboxDlqRepository outboxDlqRepository;
    @Autowired
    private ApplicationRepository applicationRepository;
    @Autowired
    private OutboxCleanupJob cleanupJob;

    @BeforeEach
    void cleanState() {
        outboxRepository.deleteAll();
        outboxDlqRepository.deleteAll();
        applicationRepository.deleteAll();
    }

    @Test
    void findPending_returnsOnlyDuePendingRows() {
        Instant now = Instant.now();
        OutboxEvent due = persistPending(now.minus(1, ChronoUnit.MINUTES));
        persistPending(now.plus(1, ChronoUnit.HOURS));
        persistPublished(now);

        List<OutboxEvent> pending = outboxRepository.findPending(now, PageRequest.of(0, 50));

        assertThat(pending).hasSize(1);
        assertThat(pending.getFirst().getId()).isEqualTo(due.getId());
    }

    @Test
    void purgeExpiredRows_deletesOnlyRowsPastRetentionWindow() {
        Instant now = Instant.now();
        OutboxEvent expiredPublished = persistPublished(now.minus(4, ChronoUnit.DAYS));
        OutboxEvent recentPublished = persistPublished(now.minus(1, ChronoUnit.HOURS));

        OutboxDlq expiredDlq = outboxDlqRepository.save(
                OutboxDlqTestDataFactory.dlqFailedAt(now.minus(31, ChronoUnit.DAYS)));
        OutboxDlq recentDlq = outboxDlqRepository.save(
                OutboxDlqTestDataFactory.dlqFailedAt(now.minus(1, ChronoUnit.HOURS)));

        cleanupJob.purgeExpiredRows();

        assertThat(outboxRepository.findById(expiredPublished.getId())).isEmpty();
        assertThat(outboxRepository.findById(recentPublished.getId())).isPresent();
        assertThat(outboxDlqRepository.findById(expiredDlq.getId())).isEmpty();
        assertThat(outboxDlqRepository.findById(recentDlq.getId())).isPresent();
    }

    private OutboxEvent persistPending(Instant scheduledRetryAt) {
        Application application = persistApplication();
        OutboxEvent event = OutboxEventTestDataFactory.pending(0, scheduledRetryAt);
        alignToApplication(event, application);
        return outboxRepository.save(event);
    }

    private OutboxEvent persistPublished(Instant publishedAt) {
        Application application = persistApplication();
        OutboxEvent event = OutboxEventTestDataFactory.published(publishedAt);
        alignToApplication(event, application);
        return outboxRepository.save(event);
    }

    private Application persistApplication() {
        return applicationRepository.save(ApplicationTestDataFactory.validApplication());
    }

    private static void alignToApplication(OutboxEvent event, Application application) {
        event.setApplicationId(application.getApplicationId());
        event.setCorrelationId(application.getCorrelationId());
    }
}
