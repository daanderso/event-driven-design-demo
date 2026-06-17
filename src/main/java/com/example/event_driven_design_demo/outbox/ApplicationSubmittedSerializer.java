package com.example.event_driven_design_demo.outbox;

import com.example.event_driven_design_demo.entity.Application;
import com.example.event_driven_design_demo.events.ApplicationSubmitted;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Component
public class ApplicationSubmittedSerializer {

    private static final int SCHEMA_VERSION = 1;

    public byte[] serialize(Application application) {
        ApplicationSubmitted event = ApplicationSubmitted.newBuilder()
                .setApplicationId(application.getApplicationId().toString())
                .setCorrelationId(application.getCorrelationId().toString())
                .setTimestamp(application.getCreatedAt().toString())
                .setFirstName(application.getFirstName())
                .setLastName(application.getLastName())
                .setVersion(SCHEMA_VERSION)
                .build();
        return serialize(event);
    }

    public byte[] serialize(ApplicationSubmitted event) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<ApplicationSubmitted> writer = new SpecificDatumWriter<>(ApplicationSubmitted.class);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(event, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize ApplicationSubmitted event", e);
        }
    }
}
