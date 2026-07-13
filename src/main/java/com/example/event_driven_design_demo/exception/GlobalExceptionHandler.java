package com.example.event_driven_design_demo.exception;

import com.example.event_driven_design_demo.outbox.replay.OutboxReplayException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import jakarta.validation.ConstraintViolationException;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({ MethodArgumentNotValidException.class })
    protected ResponseEntity<ApiError> handleValidationException(MethodArgumentNotValidException ex, WebRequest request) {
        String message = ex.getBindingResult().getFieldErrors().stream().findFirst().map(fe -> fe.getField() + ": " + fe.getDefaultMessage()).orElse(ex.getMessage());
        ApiError error = new ApiError("VALIDATION_ERROR", message);
        return new ResponseEntity<>(error, new HttpHeaders(), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler({ ConstraintViolationException.class })
    protected ResponseEntity<ApiError> handleConstraintViolation(ConstraintViolationException ex) {
        ApiError error = new ApiError("VALIDATION_ERROR", ex.getMessage());
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(OutboxReplayException.class)
    protected ResponseEntity<ApiError> handleOutboxReplay(OutboxReplayException ex) {
        ApiError error = new ApiError("NOT_FOUND", ex.getMessage());
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(CallNotPermittedException.class)
    protected ResponseEntity<ApiError> handleCircuitOpen(CallNotPermittedException ex) {
        log.warn("Circuit breaker open, rejecting call: {}", ex.getMessage());
        ApiError error = new ApiError("SERVICE_UNAVAILABLE", "Service temporarily unavailable, please retry shortly");
        return new ResponseEntity<>(error, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @ExceptionHandler({ TransientDataAccessException.class, DataAccessResourceFailureException.class })
    protected ResponseEntity<ApiError> handleTransientDataAccess(Exception ex) {
        log.warn("Transient data access failure after retries: {}", ex.getMessage());
        ApiError error = new ApiError("SERVICE_UNAVAILABLE", "Service temporarily unavailable, please retry shortly");
        return new ResponseEntity<>(error, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @ExceptionHandler(Exception.class)
    protected ResponseEntity<ApiError> handleGeneral(Exception ex) {
        log.error("Unhandled exception:", ex);
        ApiError error = new ApiError("INTERNAL_ERROR", "An unexpected error occurred");
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}

