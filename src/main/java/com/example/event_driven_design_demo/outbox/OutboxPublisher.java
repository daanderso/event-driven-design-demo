package com.example.event_driven_design_demo.outbox;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.event_driven_design_demo.entity.Outbox;



@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final String EVENT_TYPE = "ApplicationSubmitted";
    private static final String SCHEMA_VERSION = "1";
    private static final long SEND_TIMEOUT_SECONDS = 30;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final OutboxProperties outboxProperties;

    public OutboxPublisher(KafkaTemplate<String, byte[]> kafkaTemplate, OutboxProperties outboxProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.outboxProperties = outboxProperties;
    }

    public void publish(Outbox outbox) {
        publishPayload(
                outbox.getApplicationId(),
                outbox.getCorrelationId(),
                outbox.getPayload(),
                outbox.getId(),
                outbox.getAttempts());
    }

    public void publishPayload(UUID applicationId, UUID correlationId, byte[] payload) {

        publishPayload(applicationId, correlationId, payload, null, null);
        
    }

    private void publishPayload(UUID applicationId, UUID correlationId, byte[] payload, Long outboxId, Integer attempt) {
        String topic = outboxProperties.getTopic().getApplicationSubmitted();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, applicationId.toString(), payload);

        record.headers().add(new RecordHeader("correlationId", correlationId.toString().getBytes(StandardCharsets.UTF_8)));

        record.headers().add(new RecordHeader("applicationId", applicationId.toString().getBytes(StandardCharsets.UTF_8)));

        record.headers().add(new RecordHeader("eventType", EVENT_TYPE.getBytes(StandardCharsets.UTF_8)));

        record.headers().add(new RecordHeader("schemaVersion", SCHEMA_VERSION.getBytes(StandardCharsets.UTF_8)));


        try {

            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Published to Kafka topic={} applicationId={} correlationId={} outboxId={} attempt={} status=success",
                    topic, applicationId, correlationId, outboxId, attempt);

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new OutboxPublishException("Interrupted while publishing to Kafka", e);

        } catch (ExecutionException | TimeoutException e) {

            log.warn("Kafka publish failed topic={} applicationId={} correlationId={} outboxId={} attempt={} status=retry error={}",

                    topic, applicationId, correlationId, outboxId, attempt, rootMessage(e));

            throw new OutboxPublishException("Failed to publish to Kafka", e);

        }

    }


    private static String rootMessage(Throwable throwable) {

        Throwable cause = throwable.getCause();
        if (cause == null) {
            cause = throwable;
        }

        String message = cause.getMessage();

        return message != null ? message : cause.getClass().getSimpleName();

    }

}


