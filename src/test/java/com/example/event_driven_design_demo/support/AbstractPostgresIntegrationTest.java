package com.example.event_driven_design_demo.support;

import org.junit.jupiter.api.Tag;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for Postgres-parity / concurrency integration tests.
 *
 * <p>These tests are OPT-IN: they are tagged {@code "testcontainers"} (excluded from the default
 * {@code mvn test} build) and additionally self-skip via {@code disabledWithoutDocker} when no
 * Docker daemon is available. Enable them with {@code mvn test -Ptestcontainers}. See
 * {@code docs/07-test-strategy.md} (Approved Decisions).
 *
 * <p>Hibernate creates the schema ({@code ddl-auto=create-drop}) so the entity mappings and the
 * database types stay consistent (the H2 {@code schema.sql} is not Postgres-compatible). The
 * Postgres dialect activates the {@code FOR UPDATE SKIP LOCKED} retrieval branch.
 */
@Tag("testcontainers")
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@ActiveProfiles("test")
public abstract class AbstractPostgresIntegrationTest {

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        // Let Hibernate own the schema; the H2 schema.sql must not run against Postgres.
        registry.add("spring.sql.init.mode", () -> "never");
        registry.add("outbox.dispatcher.enabled", () -> "false");
    }
}
