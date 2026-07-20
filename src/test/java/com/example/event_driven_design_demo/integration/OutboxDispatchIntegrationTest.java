package com.example.event_driven_design_demo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.outbox.dispatch.OutboxDispatchService;
import com.example.event_driven_design_demo.outbox.replay.OutboxReplayService;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import com.example.event_driven_design_demo.service.ApplicationService;

/**
 * End-to-end outbox -> Kafka publishing against an in-JVM {@code @EmbeddedKafka} broker (default build,
 * no Docker). Validates the PENDING -> PUBLISHED transition, record key/headers, and manual replay.
 * The scheduler is disabled (test profile); dispatch is invoked explicitly for determinism.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"application-submitted"})
class OutboxDispatchIntegrationTest {

    @Autowired
    private ApplicationService applicationService;
    @Autowired
    private OutboxDispatchService dispatchService;
    @Autowired
    private OutboxReplayService replayService;
    @Autowired
    private OutboxRepository outboxRepository;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void submitThenDispatch_publishesToKafkaWithKeyAndHeaders() {
        ApplicationResponse response = applicationService.submitApplication(ApplicationTestDataFactory.validRequest());
        String applicationId = response.getApplicationId();

        // A PENDING outbox row exists before dispatch (transactional outbox side effect).
        OutboxEvent pending = outboxRowFor(applicationId);
        assertThat(pending.getStatus()).isEqualTo(OutboxStatus.PENDING.name());

        int processed = dispatchService.dispatchOnce();
        assertThat(processed).isEqualTo(1);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(outboxRowFor(applicationId).getStatus()).isEqualTo(OutboxStatus.PUBLISHED.name()));
        assertThat(outboxRowFor(applicationId).getPublishedAt()).isNotNull();

        ConsumerRecord<String, byte[]> record = consumeFirstFor(applicationId);
        assertThat(record).isNotNull();
        assertThat(record.value()).isNotEmpty();
        assertThat(headerValue(record, "applicationId")).isEqualTo(applicationId);
        assertThat(headerValue(record, "eventType")).isEqualTo("ApplicationSubmitted");
        assertThat(headerValue(record, "schemaVersion")).isEqualTo("1");
    }

    @Test
    void replayOutbox_republishesPublishedRowWithSameKey() {
        ApplicationResponse response = applicationService.submitApplication(ApplicationTestDataFactory.validRequest());
        String applicationId = response.getApplicationId();
        dispatchService.dispatchOnce();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(outboxRowFor(applicationId).getStatus()).isEqualTo(OutboxStatus.PUBLISHED.name()));
        drain();

        Long outboxId = outboxRowFor(applicationId).getId();
        replayService.replayOutbox(outboxId);

        ConsumerRecord<String, byte[]> record = consumeFirstFor(applicationId);
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(applicationId);
    }

    private OutboxEvent outboxRowFor(String applicationId) {
        UUID id = UUID.fromString(applicationId);
        return outboxRepository.findAll().stream()
                .filter(o -> id.equals(o.getApplicationId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No outbox row for application " + applicationId));
    }

    private ConsumerRecord<String, byte[]> consumeFirstFor(String applicationId) {
        try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("application-submitted"));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (applicationId.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        return null;
    }

    private void drain() {
        try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("application-submitted"));
            consumer.poll(Duration.ofSeconds(2));
        }
    }

    private static String headerValue(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private KafkaConsumer<String, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
