package com.example.event_driven_design_demo.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.UUID;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.events.ApplicationSubmitted;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;

/**
 * Unit tests for Avro serialization: round-trip fidelity and null-field failure behavior.
 */
class ApplicationSubmittedSerializerTest {

    private final ApplicationSubmittedSerializer serializer = new ApplicationSubmittedSerializer();

    @Test
    void serialize_producesNonEmptyBytesThatRoundTrip() throws IOException {
        Application application = ApplicationTestDataFactory.applicationWith(UUID.randomUUID(), UUID.randomUUID());

        byte[] bytes = serializer.serialize(application);

        assertThat(bytes).isNotEmpty();
        ApplicationSubmitted decoded = deserialize(bytes);
        assertThat(decoded.getApplicationId().toString()).isEqualTo(application.getApplicationId().toString());
        assertThat(decoded.getCorrelationId().toString()).isEqualTo(application.getCorrelationId().toString());
        assertThat(decoded.getTimestamp().toString()).isEqualTo(application.getCreatedAt().toString());
        assertThat(decoded.getFirstName().toString()).isEqualTo(application.getFirstName());
        assertThat(decoded.getLastName().toString()).isEqualTo(application.getLastName());
        assertThat(decoded.getVersion()).isEqualTo(1);
    }

    @Test
    void serialize_event_roundTrips() throws IOException {
        ApplicationSubmitted event = ApplicationSubmitted.newBuilder()
                .setApplicationId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setTimestamp("2026-07-13T12:00:00Z")
                .setFirstName("Ada")
                .setLastName("Lovelace")
                .setVersion(1)
                .build();

        ApplicationSubmitted decoded = deserialize(serializer.serialize(event));

        assertThat(decoded.getFirstName().toString()).isEqualTo("Ada");
        assertThat(decoded.getLastName().toString()).isEqualTo("Lovelace");
    }

    @Test
    void serialize_withNullRequiredField_throwsNullPointerException() {
        Application application = ApplicationTestDataFactory.validApplication();
        application.setApplicationId(null);

        assertThatThrownBy(() -> serializer.serialize(application))
                .isInstanceOf(NullPointerException.class);
    }

    private static ApplicationSubmitted deserialize(byte[] bytes) throws IOException {
        DatumReader<ApplicationSubmitted> reader = new SpecificDatumReader<>(ApplicationSubmitted.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }
}
