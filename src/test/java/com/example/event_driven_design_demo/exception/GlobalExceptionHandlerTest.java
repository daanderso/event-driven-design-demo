package com.example.event_driven_design_demo.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;

import com.example.event_driven_design_demo.outbox.replay.OutboxReplayException;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import jakarta.validation.ConstraintViolationException;

/**
 * Unit tests for exception-to-HTTP mapping, the heart of the error contract
 * ({@code errorCode}/{@code message}, statuses 400/404/500/503).
 */
class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void methodArgumentNotValid_maps400ValidationError() {
        MethodArgumentNotValidException ex = mock(MethodArgumentNotValidException.class);
        BindingResult bindingResult = mock(BindingResult.class);
        when(ex.getBindingResult()).thenReturn(bindingResult);
        when(bindingResult.getFieldErrors())
                .thenReturn(List.of(new FieldError("applicationRequest", "firstName", "must not be blank")));

        ResponseEntity<ApiError> response = handler.handleValidationException(ex, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().getErrorCode()).isEqualTo("VALIDATION_ERROR");
        assertThat(response.getBody().getMessage()).isEqualTo("firstName: must not be blank");
    }

    @Test
    void constraintViolation_maps400ValidationError() {
        ConstraintViolationException ex = new ConstraintViolationException("bad value", null);

        ResponseEntity<ApiError> response = handler.handleConstraintViolation(ex);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().getErrorCode()).isEqualTo("VALIDATION_ERROR");
    }

    @Test
    void outboxReplayException_maps404NotFound() {
        ResponseEntity<ApiError> response = handler.handleOutboxReplay(new OutboxReplayException("row not found"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().getErrorCode()).isEqualTo("NOT_FOUND");
        assertThat(response.getBody().getMessage()).isEqualTo("row not found");
    }

    @Test
    void circuitOpen_maps503ServiceUnavailable() {
        CallNotPermittedException ex = mock(CallNotPermittedException.class);

        ResponseEntity<ApiError> response = handler.handleCircuitOpen(ex);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().getErrorCode()).isEqualTo("SERVICE_UNAVAILABLE");
    }

    @Test
    void transientDataAccess_maps503ServiceUnavailable() {
        ResponseEntity<ApiError> response =
                handler.handleTransientDataAccess(new TransientDataAccessResourceException("db blip"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().getErrorCode()).isEqualTo("SERVICE_UNAVAILABLE");
    }

    @Test
    void dataAccessResourceFailure_maps503ServiceUnavailable() {
        ResponseEntity<ApiError> response =
                handler.handleTransientDataAccess(new DataAccessResourceFailureException("connection lost"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().getErrorCode()).isEqualTo("SERVICE_UNAVAILABLE");
    }

    @Test
    void genericException_maps500InternalError() {
        ResponseEntity<ApiError> response = handler.handleGeneral(new RuntimeException("boom"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody().getErrorCode()).isEqualTo("INTERNAL_ERROR");
        assertThat(response.getBody().getMessage()).isEqualTo("An unexpected error occurred");
    }
}
