# Event-Driven Design Demo

A Spring Boot demonstration of application submission with a **transactional outbox** pattern. Submissions are persisted to H2 and an Avro-encoded outbox record is written in the same transaction; a background outbox dispatcher then publishes those events to Kafka asynchronously.

## Tech stack

- Java 25
- Spring Boot 4.1.0
- Spring Cloud 2025.1.2 (Oakwood)
- Spring Data JPA + H2 (dev) / PostgreSQL (prod target)
- Spring Kafka (outbox dispatcher → `application-submitted` topic)
- Apache Avro (in-repo schemas, Maven code generation)
- springdoc-openapi 3.0.3 (Swagger UI)
- Maven

On classpath but not wired: Resilience4j circuit breaker (reserved for future REST resilience).

## Quick start

```bash
./mvnw spring-boot:run
```

On Windows:

```bash
mvnw.cmd spring-boot:run
```

The app starts on port 8080 with an in-memory H2 database initialized from `schema.sql` and `data.sql`.

For end-to-end Kafka verification, start the local Docker Kafka stack first — see [docker-readme.md](docker-readme.md). The app runs without Kafka, but publish attempts will fail and retry until a broker is available.

### Submit an application

```bash
curl -X POST http://localhost:8080/applications \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Jane","lastName":"Doe"}'
```

Example response:

```json
{
  "applicationId": "...",
  "correlationId": "...",
  "timestamp": "2026-06-06T12:00:00.123456789Z",
  "status": "SUBMITTED"
}
```

### H2 console

Browse the database at [http://localhost:8080/h2-console](http://localhost:8080/h2-console):

- JDBC URL: `jdbc:h2:mem:testdb`
- Username: `sa`
- Password: *(empty)*

## Documentation

| Document | Description |
|----------|-------------|
| [docs/01-requirements.md](docs/01-requirements.md) | Requirements and approved decisions |
| [docs/02-architecture.md](docs/02-architecture.md) | Architecture, data flows, implementation status |
| [docs/03-domain-model.md](docs/03-domain-model.md) | Entities, schema, Avro, persistence |
| [docs/04-api-design.md](docs/04-api-design.md) | REST API contract and DTOs |
| [docs/05-kafka-outbox-design.md](docs/05-kafka-outbox-design.md) | Kafka producer, outbox dispatch, retry/DLQ, replay |
| [docker-readme.md](docker-readme.md) | Local Docker Kafka setup and verification |

## Implementation status

| Component | Status |
|-----------|--------|
| POST /applications | Done |
| Transactional outbox write (Avro payload) | Done |
| Outbox dispatcher → Kafka | Done |
| Retry / DLQ / cleanup jobs | Done |
| DLQ admin / replay API | Done |
| Integration test (`@EmbeddedKafka`) | Done |
| GET /applications/{id} | Deferred |
| Application fallback replay REST endpoint | Not started (service method only) |
| Testcontainers integration tests | Not started |

See [docs/02-architecture.md](docs/02-architecture.md) for remaining work.

## Project layout

```
src/main/java/.../
  controller/     REST endpoints (ApplicationController, OutboxAdminController)
  service/        Business logic + outbox write
  outbox/         Dispatcher, publisher, retry, DLQ, cleanup, replay
  config/         Kafka producer configuration
  repository/     Spring Data JPA
  entity/         Application, OutboxEvent, OutboxDlq
  dto/            Request/response and admin DTOs
  exception/      Global exception handling

src/main/resources/
  avro/           Avro schema files
  schema.sql      H2 table definitions
  data.sql        Sample seed data
  application.yml Spring Boot configuration
```

## Build and test

Use this as the canonical automated check (CI, dependency upgrades, agents):

```bash
./mvnw clean verify
```

This runs all tests in-process—no separate server is started and no ports remain open afterward. Coverage includes:

- Context startup (`EventDrivenDesignDemoApplicationTests`)
- HTTP + outbox + Kafka path (`OutboxPublishIntegrationTest` with `@EmbeddedKafka`)

Use `./mvnw spring-boot:run` only for local development and manual exploration (Swagger UI, H2 console, curl). Stop it with Ctrl+C or your IDE's Stop button when finished.

For end-to-end verification against a real Kafka broker, start the Docker stack from [docker-readme.md](docker-readme.md) and run the app manually.

If port 8080 stays occupied after testing, a leftover `spring-boot:run` or IDE run is usually the cause—not `mvn verify`. Find the listener with `netstat -ano | grep :8080` and stop that process, or use Ctrl+C / IDE Stop.

Avro Java classes are generated at build time into `target/generated-sources/avro`.

Swagger UI is available at [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) when the app is running.
