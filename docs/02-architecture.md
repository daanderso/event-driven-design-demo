# 02 - Architecture

## Implementation Status (as of 2026-07-13)

| Component | Status |
|-----------|--------|
| POST /applications + validation | Implemented |
| Application + Outbox transactional write | Implemented |
| Avro schema + Maven code generation | Implemented |
| JPA entities, repositories, H2 init scripts | Implemented |
| Outbox dispatcher → Kafka | Implemented |
| Retry / DLQ move | Implemented |
| Retention cleanup jobs | Implemented |
| DLQ admin / replay API | Implemented |
| GET /applications/{id} | Deferred |
| Integration test (`@EmbeddedKafka`) | Implemented |
| Integration tests (Testcontainers Postgres, opt-in) | Implemented |

See `docs/04-api-design.md` for the REST layer and `docs/03-domain-model.md` for persistence details.

## Overview

This document presents the high-level architecture derived from `docs/01-requirements.md` and the Approved Decisions recorded there. Blocking questions have been resolved. The outbox processor, Kafka producer, retry/DLQ, cleanup, and admin replay components described below are implemented in the `outbox/` package.

## Architecture Diagram (high-level, conceptual)

 (Client)
    |
    |  HTTP REST request (submit application)
    v
 [API Layer / Controller]  ✅ Implemented
    |
    |  validate -> service call
    v
 [Service Layer]  ✅ Implemented (persist + outbox write)
    |
    |  persists application + write outbox record (single DB transaction)
    v
 [Database]  ✅ Implemented
    |-- Applications table
    |-- Outbox table (pending events, status, retries, payload)
    |-- Outbox DLQ table

 Outbox processor (background worker)  ✅ Implemented
    |-- reads from Outbox table (safe for concurrent runners)
    |-- publishes to Kafka
    |-- marks outbox row as published / increments retry metadata

 [Kafka Cluster]  ✅ Implemented
    |-- Topic: `application-submitted` (Avro-encoded events, partitioned by `applicationId`)

 Replay/Operator tooling  ✅ Partially implemented -> admin DLQ/outbox replay done; application fallback replay service-only (no REST endpoint)

Notes:
- Events are Avro-encoded. Avro schemas are stored in-repo under `src/main/resources/avro/` and versioned. No external schema registry is used for the exercise; CI should validate compatibility when added.
- The outbox table and DLQ table are part of the primary RDBMS (H2 dev / PostgreSQL prod).
- Events for the same `applicationId` are published using `applicationId` as the Kafka partition key to preserve per-application ordering.

## Components

- API Layer / Controller **(Implemented)**
  - Thin REST controller that accepts submission requests, validates input, and delegates to service.
  - Returns applicationId, correlationId, timestamp on success.
  - Admin controller (`OutboxAdminController`) exposes DLQ listing and replay endpoints under `/admin`.

- Service Layer **(Implemented)**
  - Business orchestration: generate IDs, timestamps and correlationId, construct persistable entity and outbox record, serialize Avro payload, and save them within a single transaction.
  - Keeps controller thin; contains application-level validation and simple business rules.
  - Kafka publishing is **not** done here; that is the outbox processor's responsibility.

- Persistence (Database) **(Implemented)**
  - Primary storage for Application entities.
  - Outbox table storing events to publish: payload, status, attempts, lastError, scheduledRetryAt, publishedAt, createdAt, correlationId.
  - Outbox DLQ table for permanently failed events.
  - Must be JPA-compatible and avoid H2-specific constructs to remain compatible with PostgreSQL.

- Outbox Processor (publisher) **(Implemented)**
  - Background worker (`OutboxDispatcher`, `OutboxProcessor`, `OutboxPublisher`) that queries the outbox table for pending events and publishes them to Kafka.
  - Concurrency-safe retrieval via `OutboxRetrievalService` (Postgres `FOR UPDATE SKIP LOCKED`; H2 fallback).
  - Implements retry/backoff and marks permanently failed items for operator intervention or DLQ storage.

- Kafka **(Implemented)**
  - Receives ApplicationSubmitted events encoded with Avro. Producers attach `correlationId` and `applicationId` as headers where supported.
  - Topic: `application-submitted`, partitioned by `applicationId` to guarantee ordering per application.
  - Schema files are stored in-repo; CI should run Avro compatibility checks when added.

- Replay Tooling **(Partially implemented)**
  - Admin API to re-publish events from outbox or DLQ rows (`OutboxAdminController`, `OutboxReplayService`).
  - Application-table fallback replay exists as a service method (`replayFromApplication`) but has no REST endpoint yet.
  - Must be able to rehydrate event payload and ensure idempotency requirements are respected.

- Observability / Admin **(Partially implemented)**
  - Structured logging including correlationId and applicationId is in place.
  - DLQ admin endpoints implemented; Swagger UI available via springdoc at `/swagger-ui.html`.
  - Metrics for outbox queue size, publish successes/failures, retries — not yet implemented.

## Sequence Flows

### 1) Submit Application (happy path) — Implemented

- Client -> Controller: POST /applications { firstName, lastName }
- Controller -> Service: validate input (Jakarta Validation), generate `applicationId` (UUID), `correlationId` (UUID), `timestamp` (ISO-8601 UTC).
- Service -> DB (single transaction): insert `applications` record and insert `outbox` record with `status = PENDING`, Avro-encoded `payload`, `content_type = avro/binary`, `attempts = 0`, `scheduled_retry_at = now()`.
- Service -> Controller: return 201 Created with `applicationId`, `correlationId`, `timestamp`, `status`.
- Outbox Processor (async): periodically retrieve pending outbox rows (where `status = PENDING` and `scheduled_retry_at <= now()`), publish to Kafka (topic `application-submitted` using `applicationId` as partition key). On success update outbox `status = PUBLISHED`, set `published_at` and keep row for retention window (3 days).

### 2) Outbox publish failure — Implemented

- Outbox Processor publishes and encounters transient failure -> increment `attempts`, record `last_error`, compute `scheduled_retry_at = now() + backoff(attempts)` where backoff sequence is [1s, 2s, 4s].
- Processor skips rows where `scheduled_retry_at` is in the future.
- If `attempts` exceeds 3, move the row to `outbox_dlq` (persist failure details) and mark original outbox `status = FAILED`. Operators are notified via logs and can view DLQ via admin endpoint and trigger manual replay.

### 3) Replay — Partially implemented

- Manual replay: operator lists DLQ or `PUBLISHED`/`FAILED` outbox rows via admin endpoint, selects items and triggers re-publish; processor publishes with the same `applicationId` partition key to preserve ordering. **Implemented.**
- Automatic replay for failed items: system automatically re-attempts according to `scheduled_retry_at` until attempts exhausted. **Implemented.**
- For events older than the outbox retention window (3 days), operators can reconstruct events from the `applications` table via `OutboxReplayService.replayFromApplication()` — service method only; REST endpoint not yet exposed.

## Data Flow (high level)

- Request DTO validated -> Service constructs domain model and Avro event record. **(Implemented)**
- Service persists `applications` and an `outbox` record in a single DB transaction. **(Implemented)**
- Outbox Processor reads pending rows, publishes Avro payload to Kafka (`application-submitted`) using `applicationId` as partition key, and updates outbox status (`PUBLISHED` / `FAILED`). **(Implemented)**
- Published outbox rows are retained for 3 days, then the cleanup job purges them; DLQ rows kept for 30 days. **(Implemented)**

## Design Decisions (derivable from requirements)

- Use transactional outbox pattern: application record write and outbox record write occur atomically in one DB transaction to avoid inconsistency between DB and published events.
- Keep controllers thin and place business logic in services.
- Persist correlationId alongside application and outbox entries to support tracing and auditing.
- Use H2 for development and design the schema and JPA usage to be compatible with PostgreSQL.
- Use Avro for events with in-repo schema files and CI validation.
- Support at-least-once semantics. Consumers must be idempotent and use `applicationId` as deduplication key.
- Retain published outbox rows for 3 days for audit/replay and purge older rows daily. Move permanently failed events to a DLQ table retained for 30 days.

## Tradeoffs

- Outbox vs Two-Phase Commit to Kafka: Outbox pattern avoids distributed transactions and is simpler to operate but introduces eventual consistency between DB transaction and Kafka publish; it requires an additional component (outbox processor) and careful handling of concurrency and retries.
- Serialization (Avro without registry): chosen for schema evolution benefits. Operational risk: without a registry, schema changes must be coordinated and validated in CI. This tradeoff reduces infrastructure requirements for the exercise.
- Replay source: Replaying from the outbox provides a clear single source of truth for what was intended to be published; replaying from the application table may require re-constructing event payloads and may lose historical metadata. Choosing one impacts storage and retention policy.
- Ordering guarantees: Per-application ordering is required. Using `applicationId` as the Kafka partition key satisfies this at the cost of fixed partition assignment per application.

## Resolved Decisions

All previously blocking items have been resolved and incorporated into the architecture as follows:

- Topic: `application-submitted` (single topic) — Avro-encoded events.
- Serialization: Avro, schemas stored in-repo under `src/main/resources/avro/`. CI validates compatibility; no external schema registry for the exercise.
- Delivery: at-least-once; consumers must be idempotent using `applicationId` as deduplication key.
- Replay: Outbox is the authoritative source for pending events. Published outbox rows are retained for 3 days for operator replay; older replays must be reconstructed from `applications` table.
- Retry/DLQ: 3 retry attempts with exponential backoff (1s, 2s, 4s). After retries exhausted, move to `outbox_dlq` and retain DLQ rows for 30 days.
- Circuit breaker: Resilience4j applies to REST APIs only; not used for Kafka outbox publishing (see `docs/05-kafka-outbox-design.md`).
- Ordering: Use `applicationId` as Kafka partition key to preserve ordering for events with the same id.
- DB: RDBMS (H2 dev / PostgreSQL prod). Outbox, applications, and DLQ are relational tables with appropriate indexes.

## Remaining work

1. `GET /applications/{id}` — deferred submission API endpoint.
2. REST endpoint for `OutboxReplayService.replayFromApplication()` (application-table fallback replay).
3. Optional Testcontainers Kafka variant for production parity (Postgres concurrency integration tests are implemented, opt-in via `mvn test -Ptestcontainers`).
4. Outbox metrics / observability (queue size, publish success/failure counters).
5. Wire Resilience4j for REST (if desired).
6. CI Avro compatibility checks.

---

References
- `docs/01-requirements.md` (source of truth for requirements)
