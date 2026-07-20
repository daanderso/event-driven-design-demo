package com.example.event_driven_design_demo.outbox.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.example.event_driven_design_demo.entity.OutboxEvent;
import com.example.event_driven_design_demo.fixtures.OutboxEventTestDataFactory;
import com.example.event_driven_design_demo.outbox.config.OutboxProperties;

/**
 * Unit tests for Kafka record assembly and failure wrapping, using a mocked {@link KafkaTemplate}.
 */
@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    private final OutboxProperties outboxProperties = new OutboxProperties();

    private OutboxPublisher publisher() {
        return new OutboxPublisher(kafkaTemplate, outboxProperties);
    }

    @Test
    void publish_buildsRecordWithKeyValueTopicAndHeaders() throws Exception {
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher().publish(event);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, byte[]> record = captor.getValue();

        assertThat(record.topic()).isEqualTo("application-submitted");
        assertThat(record.key()).isEqualTo(event.getApplicationId().toString());
        assertThat(record.value()).isEqualTo(event.getPayload());
        assertThat(headerValue(record, "correlationId")).isEqualTo(event.getCorrelationId().toString());
        assertThat(headerValue(record, "applicationId")).isEqualTo(event.getApplicationId().toString());
        assertThat(headerValue(record, "eventType")).isEqualTo("ApplicationSubmitted");
        assertThat(headerValue(record, "schemaVersion")).isEqualTo("1");
    }

    @Test
    void publish_wrapsExecutionExceptionAsOutboxPublishException() {
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> publisher().publish(event))
                .isInstanceOf(OutboxPublishException.class)
                .hasCauseInstanceOf(ExecutionException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void publish_wrapsTimeoutExceptionAsOutboxPublishException() throws Exception {
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        CompletableFuture<Object> future = mock(CompletableFuture.class);
        when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("timed out"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn((CompletableFuture) future);

        assertThatThrownBy(() -> publisher().publish(event))
                .isInstanceOf(OutboxPublishException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void publish_onInterruption_setsInterruptFlagAndWraps() throws Exception {
        OutboxEvent event = OutboxEventTestDataFactory.pending();
        CompletableFuture<Object> future = mock(CompletableFuture.class);
        when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("interrupted"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn((CompletableFuture) future);

        Throwable thrown = catchThrowable(() -> publisher().publish(event));

        assertThat(thrown).isInstanceOf(OutboxPublishException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.interrupted()).isTrue(); // also clears the flag for subsequent tests
    }

    private static String headerValue(ProducerRecord<String, byte[]> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
