package com.example.event_driven_design_demo.outbox;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.hibernate.Session;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.example.event_driven_design_demo.entity.Outbox;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.repository.OutboxRepository;

import jakarta.persistence.EntityManager;

@Service
public class OutboxClaimService {

    private final OutboxRepository outboxRepository;
    private final EntityManager entityManager;
    private final boolean postgresDialect;

    public OutboxClaimService(OutboxRepository outboxRepository,
                              EntityManager entityManager,
                              org.springframework.core.env.Environment environment) {
        this.outboxRepository = outboxRepository;
        this.entityManager = entityManager;
        String dialect = environment.getProperty("spring.jpa.database-platform", "");
        this.postgresDialect = dialect.toLowerCase().contains("postgres");
    }

    public List<Outbox> claimPending(int batchSize, Instant now) {
        if (postgresDialect) {
            return claimPendingPostgres(batchSize, now);
        }
        return claimPendingH2(batchSize, now);
    }

    //@SuppressWarnings("unchecked")
    private List<Outbox> claimPendingPostgres(int batchSize, Instant now) {
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
                        Outbox.class)
                .setParameter("status", OutboxStatus.PENDING.name())
                .setParameter("now", Timestamp.from(now))
                .setParameter("limit", batchSize)
                .getResultList();
    }

    private List<Outbox> claimPendingH2(int batchSize, Instant now) {
        return outboxRepository.findPending(now, PageRequest.of(0, batchSize));
    }
}
