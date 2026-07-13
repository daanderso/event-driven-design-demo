package com.example.event_driven_design_demo.outbox.dispatch;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.hibernate.Session;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.repository.OutboxRepository;

import jakarta.persistence.EntityManager;

@Service
public class OutboxRetrievalService {

    private final OutboxRepository outboxRepository;
    private final EntityManager entityManager;
    private final boolean postgresDialect;

    public OutboxRetrievalService(OutboxRepository outboxRepository,
                                  EntityManager entityManager,
                                  org.springframework.core.env.Environment environment) {
        this.outboxRepository = outboxRepository;
        this.entityManager = entityManager;
        String dialect = environment.getProperty("spring.jpa.database-platform", "");
        this.postgresDialect = dialect.toLowerCase().contains("postgres");
    }

    /**
     * Returns outbox rows with {@code status = PENDING} and {@code scheduled_retry_at <= now},
     * up to {@code batchSize}. On PostgreSQL, uses {@code FOR UPDATE SKIP LOCKED} so multiple
     * dispatcher instances do not process the same row concurrently. H2 uses a simple query
     * (single-instance only).
     */
    public List<OutboxEvent> retrievePendingOutboxEvents(int batchSize, Instant now) {
        if (postgresDialect) {
            return retrievePendingOutboxEventsPostgres(batchSize, now);
        }
        return retrievePendingOutboxEventsH2(batchSize, now);
    }

    private List<OutboxEvent> retrievePendingOutboxEventsPostgres(int batchSize, Instant now) {
        Session session = entityManager.unwrap(Session.class);
        return session.createNativeQuery(
                        """
                        SELECT * FROM outbox
                        WHERE status = :status
                          AND scheduled_retry_at <= :now
                        ORDER BY id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """,
                        OutboxEvent.class)
                .setParameter("status", OutboxStatus.PENDING.name())
                .setParameter("now", Timestamp.from(now))
                .setParameter("limit", batchSize)
                .getResultList();
    }

    private List<OutboxEvent> retrievePendingOutboxEventsH2(int batchSize, Instant now) {
        return outboxRepository.findPending(now, PageRequest.of(0, batchSize));
    }
}
