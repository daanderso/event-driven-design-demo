# Event-Driven Design Demo

A Spring Boot demonstration of application submission with a **transactional outbox** pattern. Submissions are persisted to H2 and an Avro-encoded outbox record is written in the same transaction; Kafka publishing is planned via a background outbox processor.

## Tech stack

- Java 25
- Spring Boot 3.5.14
- Spring Data JPA + H2 (dev) / PostgreSQL (prod target)
- Apache Avro (in-repo schemas, Maven code generation)
- Maven

Planned: Spring Kafka, outbox processor, Resilience4J circuit breaker, DLQ admin API.

## Quick start

```bash
./mvnw spring-boot:run
```

On Windows:

```bash
mvnw.cmd spring-boot:run
```

The app starts on port 8080 with an in-memory H2 database initialized from `schema.sql` and `data.sql`.

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

## Implementation status

| Component | Status |
|-----------|--------|
| POST /applications | Done |
| Transactional outbox write (Avro payload) | Done |
| Outbox processor → Kafka | Not started |
| Retry / DLQ / cleanup jobs | Not started |
| DLQ admin / replay API | Not started |

See [docs/02-architecture.md](docs/02-architecture.md) for the full backlog.

## Project layout

```
src/main/java/.../
  controller/     REST endpoints
  service/        Business logic + outbox write
  repository/     Spring Data JPA
  entity/         Application, Outbox, OutboxDlq
  dto/            Request/response DTOs
  exception/      Global exception handling

src/main/resources/
  avro/           Avro schema files
  schema.sql      H2 table definitions
  data.sql        Sample seed data
  application.yml Spring Boot configuration
```

## Build and test

```bash
./mvnw clean verify
```

Avro Java classes are generated at build time into `target/generated-sources/avro`.
