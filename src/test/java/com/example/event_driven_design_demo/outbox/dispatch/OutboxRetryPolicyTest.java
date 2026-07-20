package com.example.event_driven_design_demo.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for the pure retry/DLQ decision policy. Boundary correctness here is the highest-value
 * guard against silently losing or over-retrying events (Approved Decision: 1 initial + 3 retries,
 * backoff 1s/2s/4s, DLQ after the 4th failed attempt).
 */
class OutboxRetryPolicyTest {

    private final OutboxRetryPolicy policy = new OutboxRetryPolicy();

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void shouldMoveToDlq_isFalse_forAttemptsWithinRetryBudget(int attemptsAfterFailure) {
        assertThat(policy.shouldMoveToDlq(attemptsAfterFailure)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 5, 10})
    void shouldMoveToDlq_isTrue_afterRetryBudgetExhausted(int attemptsAfterFailure) {
        assertThat(policy.shouldMoveToDlq(attemptsAfterFailure)).isTrue();
    }

    @Test
    void nextRetryAt_firstRetry_isOneSecondOut() {
        Instant before = Instant.now();

        Instant next = policy.nextRetryAt(1);

        assertThat(next).isBetween(before.plusSeconds(1), before.plusSeconds(3));
    }

    @Test
    void nextRetryAt_secondRetry_isTwoSecondsOut() {
        Instant before = Instant.now();

        Instant next = policy.nextRetryAt(2);

        assertThat(next).isBetween(before.plusSeconds(2), before.plusSeconds(4));
    }

    @Test
    void nextRetryAt_thirdRetry_isFourSecondsOut() {
        Instant before = Instant.now();

        Instant next = policy.nextRetryAt(3);

        assertThat(next).isBetween(before.plusSeconds(4), before.plusSeconds(6));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 4, -1})
    void nextRetryAt_throws_forAttemptsOutsideBackoffSchedule(int invalidAttempts) {
        assertThatThrownBy(() -> policy.nextRetryAt(invalidAttempts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid attempts");
    }
}
