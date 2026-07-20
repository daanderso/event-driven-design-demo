package com.example.event_driven_design_demo.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Light unit test confirming the scheduled poller swallows dispatch errors so one bad cycle does not
 * kill the scheduler.
 */
@ExtendWith(MockitoExtension.class)
class OutboxDispatcherTest {

    @Mock
    private OutboxDispatchService dispatchService;

    @InjectMocks
    private OutboxDispatcher dispatcher;

    @Test
    void dispatchPendingOutboxRows_swallowsExceptions() {
        when(dispatchService.dispatchOnce()).thenThrow(new RuntimeException("dispatch failed"));

        assertThatCode(() -> dispatcher.dispatchPendingOutboxRows()).doesNotThrowAnyException();
        verify(dispatchService).dispatchOnce();
    }

    @Test
    void dispatchPendingOutboxRows_invokesDispatchOnce() {
        when(dispatchService.dispatchOnce()).thenReturn(3);

        dispatcher.dispatchPendingOutboxRows();

        verify(dispatchService).dispatchOnce();
    }
}
