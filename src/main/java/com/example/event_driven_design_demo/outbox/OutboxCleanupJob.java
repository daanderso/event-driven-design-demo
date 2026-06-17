package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
public class OutboxCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleanupJob.class);

    private final OutboxRepository outboxRepository;
    private final OutboxDlqRepository outboxDlqRepository;
    private final OutboxProperties outboxProperties;

    public OutboxCleanupJob(OutboxRepository outboxRepository,
                            OutboxDlqRepository outboxDlqRepository,
                            OutboxProperties outboxProperties) {
        this.outboxRepository = outboxRepository;
        this.outboxDlqRepository = outboxDlqRepository;
        this.outboxProperties = outboxProperties;
    }

    @Scheduled(cron = "0 0 2 * * *")
    @Transactional
    public void purgeExpiredRows() {
        Instant publishedCutoff = Instant.now().minus(outboxProperties.getRetention().getPublishedDays(), ChronoUnit.DAYS);
        Instant dlqCutoff = Instant.now().minus(outboxProperties.getRetention().getDlqDays(), ChronoUnit.DAYS);

        int deletedOutbox = outboxRepository.deletePublishedBefore(publishedCutoff);
        int deletedDlq = outboxDlqRepository.deleteFailedBefore(dlqCutoff);

        log.info("Outbox cleanup completed deletedPublishedOutbox={} deletedDlq={}", deletedOutbox, deletedDlq);
    }
}
