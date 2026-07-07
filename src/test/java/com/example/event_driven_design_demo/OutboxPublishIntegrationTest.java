package com.example.event_driven_design_demo;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.entity.OutboxStatus;
import com.example.event_driven_design_demo.repository.OutboxRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@EmbeddedKafka(partitions = 1, topics = {"application-submitted"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "outbox.dispatcher.fixed-delay-ms=300"
})
class OutboxPublishIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void submitApplication_publishesOutboxRowToKafka() {
        ApplicationRequest request = new ApplicationRequest();
        request.setFirstName("Jane");
        request.setLastName("Doe");

        ResponseEntity<ApplicationResponse> response =
                restTemplate.postForEntity("/applications", request, ApplicationResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ApplicationResponse body = Objects.requireNonNull(response.getBody());
        String applicationId = Objects.requireNonNull(body.getApplicationId());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<OutboxEvent> published = outboxRepository.findAll().stream()
                    .filter(o -> o.getApplicationId().toString().equals(applicationId))
                    .filter(o -> OutboxStatus.PUBLISHED.name().equals(o.getStatus()))
                    .toList();
            assertThat(published).hasSize(1);
        });

        try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("application-submitted"));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));

            boolean found = false;
            for (ConsumerRecord<String, byte[]> record : records) {
                if (applicationId.equals(record.key())) {
                    found = true;
                    assertThat(record.value()).isNotEmpty();
                    break;
                }
            }
            assertThat(found).isTrue();
        }
    }

    private KafkaConsumer<String, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
