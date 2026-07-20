package com.example.event_driven_design_demo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.fixtures.AvroPayloadFactory;
import com.example.event_driven_design_demo.repository.ApplicationRepository;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.serialization.ApplicationSubmittedSerializer;
import com.example.event_driven_design_demo.service.ApplicationService;

/**
 * Transactional-outbox atomicity against a real (H2) transaction: a successful submit commits both
 * the application row and its PENDING outbox row, while a failure after the first save rolls back
 * the whole unit of work so neither row is persisted. The serializer is a {@code @MockitoBean} so
 * the failure can be injected deterministically at the point between the two saves.
 */
@SpringBootTest
@ActiveProfiles("test")
class OutboxAtomicityIntegrationTest {

    @Autowired
    private ApplicationService applicationService;
    @Autowired
    private ApplicationRepository applicationRepository;
    @Autowired
    private OutboxRepository outboxRepository;
    @Autowired
    private OutboxDlqRepository outboxDlqRepository;

    @MockitoBean
    private ApplicationSubmittedSerializer serializer;

    @BeforeEach
    void cleanState() {
        outboxRepository.deleteAll();
        outboxDlqRepository.deleteAll();
        applicationRepository.deleteAll();
    }

    @Test
    void submit_commitsApplicationAndPendingOutboxRowTogether() {
        when(serializer.serialize(any(Application.class))).thenReturn(AvroPayloadFactory.samplePayload());

        ApplicationResponse response = applicationService.submitApplication(ApplicationTestDataFactory.validRequest());
        UUID applicationId = UUID.fromString(response.getApplicationId());

        assertThat(applicationRepository.findById(applicationId)).isPresent();

        List<OutboxEvent> outboxRows = outboxRepository.findAll().stream()
                .filter(o -> applicationId.equals(o.getApplicationId()))
                .toList();
        assertThat(outboxRows).hasSize(1);
        assertThat(outboxRows.getFirst().getStatus()).isEqualTo(OutboxStatus.PENDING.name());
    }

    @Test
    void submit_whenSerializationFails_rollsBackSoNeitherRowIsPersisted() {
        when(serializer.serialize(any(Application.class)))
                .thenThrow(new RuntimeException("serialization boom"));

        assertThatThrownBy(() -> applicationService.submitApplication(ApplicationTestDataFactory.validRequest()))
                .isInstanceOf(RuntimeException.class);

        assertThat(applicationRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }
}
