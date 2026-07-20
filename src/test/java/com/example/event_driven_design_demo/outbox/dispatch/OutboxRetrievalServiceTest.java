package com.example.event_driven_design_demo.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.NativeQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Pageable;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.repository.OutboxRepository;

import jakarta.persistence.EntityManager;

/**
 * Unit tests for the dialect branch selection. The actual {@code FOR UPDATE SKIP LOCKED} concurrency
 * behavior is validated in the opt-in Postgres integration test; here we only assert which query path
 * is chosen based on {@code spring.jpa.database-platform}.
 */
@ExtendWith(MockitoExtension.class)
class OutboxRetrievalServiceTest {

    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private EntityManager entityManager;
    @Mock
    private Environment environment;

    @Test
    void retrieve_withH2Dialect_usesJpaFindPending() {
        when(environment.getProperty("spring.jpa.database-platform", ""))
                .thenReturn("org.hibernate.dialect.H2Dialect");
        OutboxRetrievalService service = new OutboxRetrievalService(outboxRepository, entityManager, environment);

        Instant now = Instant.now();
        List<OutboxEvent> expected = List.of(OutboxEventTestDataFactory.pending());
        when(outboxRepository.findPending(eq(now), any(Pageable.class))).thenReturn(expected);

        List<OutboxEvent> result = service.retrievePendingOutboxEvents(10, now);

        assertThat(result).isEqualTo(expected);
        verify(outboxRepository).findPending(eq(now), any(Pageable.class));
        verifyNoInteractions(entityManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    void retrieve_withPostgresDialect_usesNativeSkipLockedQuery() {
        when(environment.getProperty("spring.jpa.database-platform", ""))
                .thenReturn("org.hibernate.dialect.PostgreSQLDialect");
        OutboxRetrievalService service = new OutboxRetrievalService(outboxRepository, entityManager, environment);

        Session session = org.mockito.Mockito.mock(Session.class);
        NativeQuery<OutboxEvent> nativeQuery = org.mockito.Mockito.mock(NativeQuery.class);
        List<OutboxEvent> expected = List.of(OutboxEventTestDataFactory.pending());

        when(entityManager.unwrap(Session.class)).thenReturn(session);
        when(session.createNativeQuery(anyString(), eq(OutboxEvent.class))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(anyString(), any())).thenReturn(nativeQuery);
        when(nativeQuery.getResultList()).thenReturn(expected);

        List<OutboxEvent> result = service.retrievePendingOutboxEvents(10, Instant.now());

        assertThat(result).isEqualTo(expected);
        verify(session).createNativeQuery(anyString(), eq(OutboxEvent.class));
        verifyNoInteractions(outboxRepository);
    }
}
