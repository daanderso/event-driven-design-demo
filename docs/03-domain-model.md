# 03 - Domain Model & Persistence

This document defines the domain entities, database schema, JPA mapping guidance and the Avro event schema for the architecture described in `docs/02-architecture.md`.

## Implementation Status (as of 2026-06-16)

| Item | Status |
|------|--------|
| JPA entities (`Application`, `Outbox`, `OutboxDlq`) | Implemented |
| Spring Data repositories | Implemented |
| Avro schema file | Implemented |
| Maven Avro plugin (`avro-maven-plugin`) | Implemented in `pom.xml` |
| H2 `schema.sql` / `data.sql` | Implemented |
| Outbox processor claiming logic | Implemented (`OutboxClaimService`) |
| Retention cleanup job | Implemented (`OutboxCleanupJob`) |

Checklist
- Use RDBMS (H2 for dev / PostgreSQL for prod)
- `applicationId` and `correlationId` are UUIDs
- Outbox pattern implemented with `outbox` and `outbox_dlq` tables
- Avro event schema stored in-repo under `src/main/resources/avro/` as `application-submitted-v1.avsc`

## Entities

### 1) Application (table: `applications`)

Fields
- applicationId : UUID (PK)
- correlationId : UUID (not null)
- firstName : varchar(50) (not null)
- lastName : varchar(50) (not null)
- status : varchar(32) (not null) — e.g., SUBMITTED
- createdAt : timestamptz / datetime (not null)
- updatedAt : timestamptz / datetime (not null)
- version : long / integer (optimistic lock)

Constraints
- `applicationId` is primary key (UUID)
- `correlationId` must be not null
- `firstName` and `lastName` max length = 50, not null
- name validation enforced at DTO/service layer (regex) and optionally via DB check constraints

Indexes
- PK on `application_id`
- Index on `correlation_id` (for lookup by correlation)

JPA mapping guidance (implemented in `Application.java`)
- Entity: Application
  - @Entity @Table(name = "applications")
  - @Id @Column(name = "application_id") private UUID applicationId;
  - @Column(name = "correlation_id") private UUID correlationId;
  - @Column(name = "first_name", length = 50) private String firstName;
  - @Column(name = "last_name", length = 50) private String lastName;
  - @Column(name = "status", length = 32) private String status;
  - @Column(name = "created_at") private Instant createdAt;
  - @Column(name = "updated_at") private Instant updatedAt;
  - @Version private Long version;

Notes
- Use constructor injection in services; keep entities as simple JPA entities.
- Avoid DB-specific SQL in JPA mappings; prefer standard JPA types to maintain H2/Postgres portability.

### 2) Outbox (table: `outbox`)

Purpose: store pending events that must be published to Kafka. The outbox record is written in the same DB transaction as the corresponding application record.

Fields
- id : BIGSERIAL / BIGINT auto-increment (PK)
- applicationId : UUID (FK to `applications.application_id`) (not null)
- correlationId : UUID (not null) — for request tracing (follows event through its lifecycle)
- payload : bytea / BLOB (Avro binary payload) (not null)
- content_type : varchar(64) (e.g., "avro/binary")
- status : varchar(16) (PENDING, PUBLISHED, FAILED) (not null)
- attempts : integer (not null, default 0)
- last_error : text (nullable)
- scheduled_retry_at : timestamptz / datetime (not null) — when the row is eligible for the next attempt
- created_at : timestamptz / datetime (not null)
- published_at : timestamptz / datetime (nullable)

Constraints
- `application_id` not null, FK referencing `applications(application_id)`
- `correlation_id` not null
- `payload` not null
- `status` not null

Indexes
- Composite index on (`status`, `scheduled_retry_at`) to quickly find pending rows
- Index on `application_id` for replay and query-by-application
- Index on `correlation_id` for tracing (present in H2 `schema.sql`)

JPA mapping guidance (implemented in `Outbox.java`)
- @Entity @Table(name = "outbox")
- @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
- @Lob @Column(name = "payload") private byte[] payload;
- Use Instant for timestamps

Claiming strategy (safe concurrency) — **implemented in `OutboxClaimService`**
- Postgres: `SELECT ... FOR UPDATE SKIP LOCKED` via native query.
- H2: single-instance fallback using `OutboxRepository.findPending(now, PageRequest)`.
- Documented limitation: concurrent multi-instance correctness is not guaranteed on H2.

Payload encoding
- Store Avro binary in `payload` as bytes (bytea / BLOB). Set `content_type = avro/binary`.
- Use the generated `ApplicationSubmitted` class from `avro-maven-plugin` for serialization via `ApplicationSubmittedSerializer` (Approved Decision; see `docs/05-kafka-outbox-design.md`).

Retention — **implemented in `OutboxCleanupJob`**
- On successful publish mark `status = PUBLISHED` and set `published_at`. Keep the record for retention window (3 days) then purge.

### 3) Outbox DLQ (table: `outbox_dlq`)

Purpose: persist permanently failed outbox rows for operator inspection and manual replay.

Fields
- id : BIGSERIAL (PK)
- original_outbox_id : BIGINT (nullable) — pointer back to the original outbox id if available
- application_id : UUID
- correlation_id : UUID — for tracing failed events back to original request
- payload : bytea / BLOB
- failure_reason : text
- failed_at : timestamptz (not null)
- attempts : integer
- created_at : timestamptz

Indexes
- Index on `failed_at` for cleanup
- Index on `application_id` for replay by application
- Index on `correlation_id` for tracing (present in H2 `schema.sql`)

Retention — **implemented in `OutboxCleanupJob`**
- Keep DLQ records for 30 days, then purge automatically.

## Database Schema (Postgres-compatible sample DDL)

```sql
-- Applications table
CREATE TABLE applications (
  application_id UUID PRIMARY KEY,
  correlation_id UUID NOT NULL,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  status VARCHAR(32) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  version BIGINT
);
CREATE INDEX idx_applications_correlation_id ON applications(correlation_id);

-- Outbox table
CREATE TABLE outbox (
  id BIGSERIAL PRIMARY KEY,
  application_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  payload BYTEA NOT NULL,
  content_type VARCHAR(64),
  status VARCHAR(16) NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  scheduled_retry_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  published_at TIMESTAMP WITH TIME ZONE NULL,
  FOREIGN KEY (application_id) REFERENCES applications(application_id)
);
CREATE INDEX idx_outbox_status_retry ON outbox(status, scheduled_retry_at);
CREATE INDEX idx_outbox_application_id ON outbox(application_id);
CREATE INDEX idx_outbox_correlation_id ON outbox(correlation_id);

-- Outbox DLQ table
CREATE TABLE outbox_dlq (
  id BIGSERIAL PRIMARY KEY,
  original_outbox_id BIGINT,
  application_id UUID,
  correlation_id UUID,
  payload BYTEA,
  failure_reason TEXT,
  attempts INTEGER,
  failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_dlq_failed_at ON outbox_dlq(failed_at);
CREATE INDEX idx_outbox_dlq_application_id ON outbox_dlq(application_id);
CREATE INDEX idx_outbox_dlq_correlation_id ON outbox_dlq(correlation_id);
```

Notes on H2 compatibility
- Use standard SQL types and JPA types supported by H2. For H2, `BYTEA` maps to `BLOB` and the JPA provider will handle mapping if `@Lob` is used.
- H2 init scripts live at `src/main/resources/schema.sql` and mirror this design with H2-compatible types.
- Avoid Postgres-specific features in application code (e.g., `RETURNING`) unless guarded by a migration or platform check.

## Avro schema

Stored as `src/main/resources/avro/application-submitted-v1.avsc`:

```json
{
  "type": "record",
  "name": "ApplicationSubmitted",
  "namespace": "com.example.event_driven_design_demo.events",
  "doc": "Avro schema for ApplicationSubmitted v1",
  "fields": [
    { "name": "applicationId", "type": "string", "doc": "UUID string" },
    { "name": "correlationId", "type": "string", "doc": "UUID string" },
    { "name": "timestamp", "type": "string", "doc": "ISO-8601 UTC timestamp" },
    { "name": "firstName", "type": "string" },
    { "name": "lastName", "type": "string" },
    { "name": "version", "type": "int", "default": 1 }
  ]
}
```

Notes for Avro usage
- Store Avro schema files in-repo and version them: `application-submitted-v1.avsc`, `application-submitted-v2.avsc`, etc.
- Producer encodes events using Avro binary encoding. Schema version is implied by the filename (`v1`).
- Without a schema registry, CI should run compatibility checks on schema changes before merging.

## Build-time generation

The project uses the Maven Avro plugin (`avro-maven-plugin` 1.11.1) configured in `pom.xml` to generate Java classes from the Avro schema at build time into `target/generated-sources/avro`. The schema file in `src/main/resources/avro/` remains the source of truth.

## Resolved choices and implementation clarifications

The following decisions are final and reflected in the codebase:

1) **Payload storage** — Avro binary in `outbox.payload` as BYTEA/BLOB; `content_type = 'avro/binary'`.

2) **Outbox.id type** — `BIGSERIAL` (Long) for efficient scanning. `application_id` and `correlation_id` are UUIDs.

3) **Claiming strategy** — Postgres: `SELECT ... FOR UPDATE SKIP LOCKED` in `OutboxClaimService`. H2: simpler single-instance fallback via `OutboxRepository.findPending`.

4) **Schema versioning** — No `schema_version` column; rely on file-based versioning (`application-submitted-v1.avsc`).

5) **Foreign key behavior** — FK from `outbox.application_id` to `applications`; no cascade deletes.

6) **Application.status** — Java enum `ApplicationStatus` (currently `SUBMITTED` only) mapped to VARCHAR.

7) **updated_at** — Set by the service layer on writes; no DB triggers.

8) **Optimistic locking** — `@Version Long version` on `Application`.

9) **Retention job** — Daily cleanup in `OutboxCleanupJob`: published outbox rows older than 3 days, DLQ rows older than 30 days.

10) **Testing strategy** — Unit tests on H2; `@EmbeddedKafka` integration test (`OutboxPublishIntegrationTest`). Concurrency tests should use Testcontainers with Postgres (not yet added).

## Completed vs remaining work

**Completed:**
- Avro schema file at `src/main/resources/avro/application-submitted-v1.avsc`
- JPA entities for `Application`, `Outbox`, and `OutboxDlq`
- Spring Data repositories (`ApplicationRepository`, `OutboxRepository`, `OutboxDlqRepository`)
- H2 initialization scripts (`schema.sql`, `data.sql`)
- Transactional outbox write in `ApplicationServiceImpl`
- Outbox processor with Postgres-friendly claiming (H2 fallback) via `OutboxClaimService`
- Configurable scheduled cleanup job for retention via `OutboxCleanupJob`
- `@EmbeddedKafka` integration test (`OutboxPublishIntegrationTest`)

**Remaining:**
- Testcontainers integration tests (Postgres + Kafka; pre-provision `application-submitted` topic)
- REST endpoint for application-table fallback replay (`OutboxReplayService.replayFromApplication()`)

## Database initialization for local H2 development

Spring Boot executes these scripts on startup for the embedded H2 database (default behavior):

- `src/main/resources/schema.sql` — creates `applications`, `outbox`, and `outbox_dlq` tables with H2-compatible types and indexes.
- `src/main/resources/data.sql` — inserts sample data: two applications, one PENDING outbox row, one PUBLISHED outbox row, and one DLQ record.

Notes:
- These scripts are intended for local development only. In higher environments use Liquibase/Flyway migrations targeting Postgres.
- `spring.jpa.hibernate.ddl-auto` is set to `validate` in `application.yml`; tables must come from `schema.sql`, not Hibernate auto-DDL.
