package com.example.event_driven_design_demo.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI eventDrivenDesignDemoOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Event-Driven Design Demo API")
                        .description("""
                                REST API for submitting applications and administering the transactional outbox.
                                Application submissions are persisted with an outbox record; a background processor
                                publishes events to Kafka asynchronously.""")
                        .version("v1")
                        .contact(new Contact().name("Event-Driven Design Demo")));
    }
}
