package com.example.event_driven_design_demo.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.dispatch.OutboxRetrievalService;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.support.AbstractPostgresIntegrationTest;

/**
 * OPT-IN Testcontainers test (tag {@code testcontainers}, inherited from
 * {@link AbstractPostgresIntegrationTest}): proves {@code FOR UPDATE SKIP LOCKED} on the Postgres
 * retrieval branch. Two concurrent transactions each retrieve a batch of PENDING rows while both
 * hold their transaction open; SKIP LOCKED must ensure the two returned id-sets are disjoint and
 * together cover every row. Runs only with {@code mvn test -Ptestcontainers} and a Docker daemon.
 *
 * <p>On Postgres the schema is Hibernate-created ({@code create-drop}) with no FK to {@code applications},
 * so outbox rows can be inserted without matching application rows (unlike the H2 tests).
 */
class OutboxRetrievalConcurrencyPostgresTest extends AbstractPostgresIntegrationTest {

    private static final int TOTAL_ROWS = 6;
    private static final int BATCH_SIZE = 3;

    @Autowired
    private OutboxRetrievalService retrievalService;
    @Autowired
    private OutboxRepository outboxRepository;
    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void concurrentRetrieval_doesNotHandOutTheSameRowTwice() throws Exception {
        Instant now = Instant.now();
        Set<Long> allIds = new HashSet<>();
        for (int i = 0; i < TOTAL_ROWS; i++) {
            OutboxEvent event = OutboxEventTestDataFactory.pending(0, now.minus(1, ChronoUnit.MINUTES));
            event.setApplicationId(UUID.randomUUID());
            event.setCorrelationId(UUID.randomUUID());
            allIds.add(outboxRepository.save(event).getId());
        }

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        CountDownLatch bothSelected = new CountDownLatch(2);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Set<Long>> first = pool.submit(() -> selectWithinTransaction(transactionTemplate, now, bothSelected));
            Future<Set<Long>> second = pool.submit(() -> selectWithinTransaction(transactionTemplate, now, bothSelected));

            Set<Long> firstIds = first.get(30, TimeUnit.SECONDS);
            Set<Long> secondIds = second.get(30, TimeUnit.SECONDS);

            assertThat(firstIds).doesNotContainAnyElementsOf(secondIds);

            Set<Long> combined = new HashSet<>(firstIds);
            combined.addAll(secondIds);
            assertThat(combined).isEqualTo(allIds);
        } finally {
            pool.shutdownNow();
        }
    }

    private Set<Long> selectWithinTransaction(TransactionTemplate transactionTemplate,
                                              Instant now,
                                              CountDownLatch bothSelected) {
        return transactionTemplate.execute(status -> {
            List<OutboxEvent> rows = retrievalService.retrievePendingOutboxEvents(BATCH_SIZE, now);
            Set<Long> ids = rows.stream().map(OutboxEvent::getId).collect(Collectors.toSet());
            // Hold the row locks open until the other transaction has also selected, so SKIP LOCKED
            // is actually exercised (each transaction must skip the rows locked by the other).
            bothSelected.countDown();
            try {
                bothSelected.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while coordinating concurrent retrieval", e);
            }
            return ids;
        });
    }
}
