package com.example.event_driven_design_demo.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.ApplicationStatus;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.serialization.ApplicationSubmittedSerializer;

/**
 * Unit tests for the transactional write orchestration: persist the application and a PENDING outbox
 * row using the serialized payload, and return a response matching the persisted values.
 */
@ExtendWith(MockitoExtension.class)
class ApplicationServiceImplTest {

    @Mock
    private ApplicationRepository applicationRepository;
    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private ApplicationSubmittedSerializer eventSerializer;

    @InjectMocks
    private ApplicationServiceImpl applicationService;

    @Test
    void submitApplication_persistsApplicationAndPendingOutboxEvent() {
        ApplicationRequest request = ApplicationTestDataFactory.requestWith("Jane", "Doe");
        byte[] payload = "avro-bytes".getBytes();
        when(eventSerializer.serialize(any(Application.class))).thenReturn(payload);

        ApplicationResponse response = applicationService.submitApplication(request);

        ArgumentCaptor<Application> applicationCaptor = ArgumentCaptor.forClass(Application.class);
        verify(applicationRepository).save(applicationCaptor.capture());
        Application savedApplication = applicationCaptor.getValue();
        assertThat(savedApplication.getFirstName()).isEqualTo("Jane");
        assertThat(savedApplication.getLastName()).isEqualTo("Doe");
        assertThat(savedApplication.getStatus()).isEqualTo(ApplicationStatus.SUBMITTED.name());
        assertThat(savedApplication.getApplicationId()).isNotNull();
        assertThat(savedApplication.getCorrelationId()).isNotNull();
        assertThat(savedApplication.getCreatedAt()).isNotNull();

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(outboxCaptor.capture());
        OutboxEvent savedOutbox = outboxCaptor.getValue();
        assertThat(savedOutbox.getStatus()).isEqualTo(OutboxStatus.PENDING.name());
        assertThat(savedOutbox.getAttempts()).isZero();
        assertThat(savedOutbox.getContentType()).isEqualTo("avro/binary");
        assertThat(savedOutbox.getPayload()).isEqualTo(payload);
        assertThat(savedOutbox.getApplicationId()).isEqualTo(savedApplication.getApplicationId());
        assertThat(savedOutbox.getCorrelationId()).isEqualTo(savedApplication.getCorrelationId());
        assertThat(savedOutbox.getScheduledRetryAt()).isNotNull();

        assertThat(response.getApplicationId()).isEqualTo(savedApplication.getApplicationId().toString());
        assertThat(response.getCorrelationId()).isEqualTo(savedApplication.getCorrelationId().toString());
        assertThat(response.getStatus()).isEqualTo(ApplicationStatus.SUBMITTED.name());
        assertThat(response.getTimestamp()).isNotBlank();
    }

    @Test
    void submitApplication_serializesTheSavedApplication() {
        ApplicationRequest request = ApplicationTestDataFactory.validRequest();
        when(eventSerializer.serialize(any(Application.class))).thenReturn("bytes".getBytes());

        applicationService.submitApplication(request);

        verify(eventSerializer).serialize(any(Application.class));
    }
}
