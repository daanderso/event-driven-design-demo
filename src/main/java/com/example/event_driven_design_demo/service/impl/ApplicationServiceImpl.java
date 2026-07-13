package com.example.event_driven_design_demo.service.impl;

import java.time.Instant;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.ApplicationStatus;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.serialization.ApplicationSubmittedSerializer;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.resilience.ResilienceInstanceNames;
import com.example.event_driven_design_demo.service.ApplicationService;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;

@Slf4j
@Service
public class ApplicationServiceImpl implements ApplicationService {

    private final ApplicationRepository applicationRepository;
    private final OutboxRepository outboxRepository;
    private final ApplicationSubmittedSerializer eventSerializer;

    public ApplicationServiceImpl(ApplicationRepository applicationRepository,
                                  OutboxRepository outboxRepository,
                                  ApplicationSubmittedSerializer eventSerializer) {
        this.applicationRepository = applicationRepository;
        this.outboxRepository = outboxRepository;
        this.eventSerializer = eventSerializer;
    }

    @Override
    @Retry(name = ResilienceInstanceNames.APPLICATION_SUBMISSION_PERSISTENCE)
    @CircuitBreaker(name = ResilienceInstanceNames.APPLICATION_SUBMISSION_PERSISTENCE)
    @Transactional
    public ApplicationResponse submitApplication(ApplicationRequest request) {
        UUID applicationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Instant now = Instant.now();

        Application application = new Application();
        application.setApplicationId(applicationId);
        application.setCorrelationId(correlationId);
        application.setFirstName(request.getFirstName());
        application.setLastName(request.getLastName());
        application.setStatus(ApplicationStatus.SUBMITTED.name());
        application.setCreatedAt(now);
        application.setUpdatedAt(now);

        applicationRepository.save(application);

        byte[] payload = eventSerializer.serialize(application);

        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setApplicationId(applicationId);
        outboxEvent.setCorrelationId(correlationId);
        outboxEvent.setPayload(payload);
        outboxEvent.setContentType("avro/binary");
        outboxEvent.setStatus(OutboxStatus.PENDING.name());
        outboxEvent.setAttempts(0);
        outboxEvent.setScheduledRetryAt(now);
        outboxEvent.setCreatedAt(now);

        outboxRepository.save(outboxEvent);

        log.info("Saved application {} and outbox event {} (correlationId={})", applicationId, outboxEvent.getId(), correlationId);

        return new ApplicationResponse(
                applicationId.toString(),
                correlationId.toString(),
                now.toString(),
                ApplicationStatus.SUBMITTED.name());
    }
}
