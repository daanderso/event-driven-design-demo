package com.example.event_driven_design_demo.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.replay.OutboxReplayReader;
import com.example.event_driven_design_demo.repository.OutboxRepository;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Proves the Resilience4j {@code @Retry} on the {@code outbox-persistence} instance actually engages
 * on {@link OutboxReplayReader#findOutboxEvent(Long)} - i.e. the reads are isolated in their own bean
 * so Spring AOP is not bypassed by self-invocation. A transient failure is thrown twice then the read
 * succeeds; the retry (max-attempts=6) must transparently recover after exactly 3 invocations.
 */
@SpringBootTest
@ActiveProfiles("test")
class OutboxReplayReaderResilienceTest {

    @Autowired
    private OutboxReplayReader replayReader;
    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @MockitoBean
    private OutboxRepository outboxRepository;

    @BeforeEach
    void resetCircuit() {
        circuitBreakerRegistry.circuitBreaker(ResilienceInstanceNames.OUTBOX_PERSISTENCE).reset();
    }

    @Test
    void findOutboxEvent_retriesTransientFailure_thenReturns() {
        long outboxId = 7L;
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        when(outboxRepository.findById(outboxId))
                .thenThrow(new TransientDataAccessResourceException("db blip 1"))
                .thenThrow(new TransientDataAccessResourceException("db blip 2"))
                .thenReturn(Optional.of(event));

        OutboxEvent result = replayReader.findOutboxEvent(outboxId);

        assertThat(result).isSameAs(event);
        // 2 transient failures + 1 success proves @Retry engages (not bypassed by self-invocation).
        verify(outboxRepository, times(3)).findById(outboxId);
    }
}
