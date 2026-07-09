package com.example.event_driven_design_demo;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.exception.ApiError;
import com.example.event_driven_design_demo.outbox.OutboxDispatchService;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.resilience.ResilienceInstanceNames;
import com.example.event_driven_design_demo.service.ApplicationService;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "outbox.dispatcher.enabled=false")
@AutoConfigureTestRestTemplate
class DbResilienceTest {

    @Autowired
    private ApplicationService applicationService;

    @Autowired
    private OutboxDispatchService outboxDispatchService;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @MockitoBean
    private ApplicationRepository applicationRepository;

    @MockitoBean
    private OutboxRepository outboxRepository;

    private CircuitBreaker submitCircuit;
    private CircuitBreaker outboxCircuit;

    @BeforeEach
    void resetCircuits() {
        submitCircuit = circuitBreakerRegistry.circuitBreaker(
                ResilienceInstanceNames.APPLICATION_SUBMISSION_PERSISTENCE);
        outboxCircuit = circuitBreakerRegistry.circuitBreaker(
                ResilienceInstanceNames.OUTBOX_PERSISTENCE);
        submitCircuit.reset();
        outboxCircuit.reset();
    }

    private static ApplicationRequest sampleRequest() {
        ApplicationRequest request = new ApplicationRequest();
        request.setFirstName("Jane");
        request.setLastName("Doe");
        return request;
    }

    @Test
    void submit_retriesTransientDataAccessException_thenSucceeds() {
        when(applicationRepository.save(any(Application.class)))
                .thenThrow(new TransientDataAccessResourceException("db blip 1"))
                .thenThrow(new TransientDataAccessResourceException("db blip 2"))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(outboxRepository.save(any(OutboxEvent.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        ApplicationResponse response = applicationService.submitApplication(sampleRequest());

        assertThat(response).isNotNull();
        assertThat(response.getApplicationId()).isNotBlank();
        // 2 failed attempts + 1 successful attempt = 3 invocations under application-submission-persistence.
        verify(applicationRepository, times(3)).save(any(Application.class));
    }

    @Test
    void submit_whenCircuitOpen_returns503ServiceUnavailable() {
        submitCircuit.transitionToOpenState();

        ResponseEntity<ApiError> response =
                restTemplate.postForEntity("/applications", sampleRequest(), ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getErrorCode()).isEqualTo("SERVICE_UNAVAILABLE");
    }

    @Test
    void openingOutboxCircuit_doesNotAffectSubmitCircuit() {
        outboxCircuit.transitionToOpenState();

        // Background dispatch is short-circuited by its own open circuit.
        assertThatThrownBy(() -> outboxDispatchService.dispatchOnce())
                .isInstanceOf(CallNotPermittedException.class);

        // The submit circuit is untouched and the synchronous path still works.
        assertThat(submitCircuit.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        when(applicationRepository.save(any(Application.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(outboxRepository.save(any(OutboxEvent.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        ApplicationResponse response = applicationService.submitApplication(sampleRequest());
        assertThat(response).isNotNull();
    }
}
