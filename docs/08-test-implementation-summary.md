# 08 - Test Implementation Summary

## Summary

This document records the implemented automated test suite for the Application Submission service and
how it maps to the approved specification in `docs/07-test-strategy.md`. The suite follows the
**opt-in Testcontainers** model (an Approved Decision): `@EmbeddedKafka` is the default for messaging
tests and everything Docker-dependent is tagged `@Tag("testcontainers")` and excluded from the default
`mvn test` run. The everyday build is therefore fast and Docker-free; the Postgres concurrency test
runs only via `mvn test -Ptestcontainers` with a running Docker daemon.

The layers implemented (or already present and retained) are:

- **Unit tests** for all business-logic classes (retry policy, processor, dispatch, retrieval, DLQ,
  replay, cleanup, serialization, submission service, publisher, exception handler).
- **API / slice tests** (MockMvc) for the two controllers, locking the HTTP contract and error shape.
- **Integration tests** (this deliverable) for end-to-end publish, retry/DLQ, transactional atomicity,
  repository queries, retention cleanup, replay, and Postgres `SKIP LOCKED` concurrency.
- **Resilience tests** for the Resilience4j DB path, including a self-invocation guard test on the
  isolated replay reader.

## Test Matrix

| Production class | Test class(es) | Layer |
|---|---|---|
| `service.impl.ApplicationServiceImpl` | `ApplicationServiceImplTest`; `OutboxAtomicityIntegrationTest` | unit + integration |
| `serialization.ApplicationSubmittedSerializer` | `ApplicationSubmittedSerializerTest` | unit |
| `outbox.dispatch.OutboxRetryPolicy` | `OutboxRetryPolicyTest` | unit |
| `outbox.dispatch.OutboxProcessor` | `OutboxProcessorTest`; `OutboxDlqIntegrationTest` | unit + integration |
| `outbox.dispatch.OutboxDispatchService` | `OutboxDispatchServiceTest`; `OutboxDispatchIntegrationTest` | unit + integration |
| `outbox.dispatch.OutboxRetrievalService` | `OutboxRetrievalServiceTest`; `OutboxRetrievalConcurrencyPostgresTest` (opt-in) | unit + integration |
| `outbox.dispatch.OutboxDispatcher` (scheduler) | `OutboxDispatcherTest`; also covered via `OutboxDispatchIntegrationTest`/`OutboxPublishIntegrationTest` | unit + integration |
| `outbox.dlq.OutboxDlqService` | `OutboxDlqServiceTest`; `OutboxDlqIntegrationTest` | unit + integration |
| `outbox.publish.OutboxPublisher` | `OutboxPublisherTest`; `OutboxDispatchIntegrationTest` | unit + integration |
| `outbox.replay.OutboxReplayService` | `OutboxReplayServiceTest`; `OutboxDispatchIntegrationTest` | unit + integration |
| `outbox.replay.OutboxReplayReader` | `OutboxReplayReaderTest`; `OutboxReplayReaderResilienceTest` | unit + resilience |
| `outbox.cleanup.OutboxCleanupJob` | `OutboxCleanupJobTest`; `OutboxRepositoryIntegrationTest` | unit + integration |
| `exception.GlobalExceptionHandler` | `GlobalExceptionHandlerTest`; controller tests | unit + API |
| `controller.ApplicationController` | `ApplicationControllerTest` | API (MockMvc) |
| `controller.OutboxAdminController` | `OutboxAdminControllerTest` | API (MockMvc) |
| `repository.OutboxRepository` (custom queries) | `OutboxRepositoryIntegrationTest`; `OutboxRetrievalConcurrencyPostgresTest` (opt-in) | integration |
| `repository.OutboxDlqRepository` (custom query) | `OutboxRepositoryIntegrationTest` | integration |
| `repository.ApplicationRepository` | exercised indirectly by integration tests | integration |
| DB resilience (submit path + circuit isolation) | `DbResilienceTest` (existing) | resilience |
| Application bootstrap | `EventDrivenDesignDemoApplicationTests` (context loads) | smoke |

Classes marked *excluded* (no dedicated test) are listed under [Excluded Classes](#excluded-classes).

## Coverage Summary (by feature)

- **Submission (synchronous write path):** unit test of orchestration + integration atomicity
  (`OutboxAtomicityIntegrationTest`) proving the application row and PENDING outbox row commit
  together, and roll back together on a mid-transaction failure.
- **Outbox dispatch:** `OutboxDispatchIntegrationTest` submits and dispatches, asserting the
  PENDING -> PUBLISHED transition and `publishedAt`.
- **Retry / backoff / DLQ:** `OutboxRetryPolicyTest` + `OutboxProcessorTest` (boundaries and backoff)
  and `OutboxDlqIntegrationTest` end-to-end: 4 forced publish failures move the row to `outbox_dlq`
  (status FAILED, attempts=4); a single failure reschedules (PENDING, attempts=1, future retry time).
- **Replay:** unit tests for the three replay flows and `OutboxDispatchIntegrationTest` republishing
  a PUBLISHED row to Kafka with the same key.
- **Serialization:** `ApplicationSubmittedSerializerTest` (Avro round-trip + error wrapping).
- **Cleanup / retention:** `OutboxCleanupJobTest` (cutoff math) + `OutboxRepositoryIntegrationTest`
  (only rows past the 3-day / 30-day windows are deleted).
- **Resilience (Resilience4j):** `DbResilienceTest` (retry + 503 on open circuit + instance isolation)
  and `OutboxReplayReaderResilienceTest` (retry engages on the isolated reader; self-invocation guard).
- **API contract:** `ApplicationControllerTest` + `OutboxAdminControllerTest` (status codes, payloads,
  validation, `{ errorCode, message }` error shape, 404 branches).
- **Messaging (Kafka):** `OutboxPublishIntegrationTest` (existing) and `OutboxDispatchIntegrationTest`
  assert the record key = `applicationId`, non-empty Avro value, and headers
  `applicationId` / `eventType=ApplicationSubmitted` / `schemaVersion=1`.
- **Concurrency (Postgres, opt-in):** `OutboxRetrievalConcurrencyPostgresTest` proves
  `FOR UPDATE SKIP LOCKED` hands each PENDING row to exactly one concurrent transaction.

## Excluded Classes

Per the testing preferences and `docs/07-test-strategy.md`, the following have no dedicated tests
because they carry no meaningful branching logic (they are validated indirectly where relevant):

- **DTOs / records:** `ApplicationRequest`, `ApplicationResponse`, `OutboxDlqResponse`,
  `ReplayResponse`, `ApiError` — plain data carriers; validation annotations on `ApplicationRequest`
  are exercised through the API tests.
- **Entities:** `Application`, `OutboxEvent`, `OutboxDlq` — persistence mappings, exercised through
  integration tests.
- **Enums:** `ApplicationStatus`, `OutboxStatus` — constant sets.
- **Config / constants:** `KafkaProducerConfig`, `OpenApiConfig`, `OutboxProperties`,
  `ResilienceInstanceNames`, `StandardServerErrorResponses` — configuration only.
- **Repository interfaces:** `ApplicationRepository` (no custom queries); custom queries on
  `OutboxRepository` / `OutboxDlqRepository` are covered by integration tests, not unit tests.
- **Exception classes:** `OutboxPublishException`, `OutboxReplayException` — no logic.
- **Generated Avro class:** `events.ApplicationSubmitted` — code-generated at build time; excluded from
  tests and from JaCoCo (`**/event_driven_design_demo/events/**`).
- **Bootstrap class:** `EventDrivenDesignDemoApplication` — covered only by the context-load smoke test.

## Implementation Notes

- **Opt-in Testcontainers gating.** The only Docker-dependent test,
  `OutboxRetrievalConcurrencyPostgresTest`, extends `AbstractPostgresIntegrationTest`, which is
  `@Tag("testcontainers")` + `@Testcontainers(disabledWithoutDocker = true)`. The default Surefire run
  excludes the `testcontainers` group (`excludedGroups=${test.excluded.groups}`, defaulting to
  `testcontainers`), so `mvn test` needs no Docker. The `testcontainers` Maven profile clears the
  exclusion. The class is named `...Test` (not `...IT`) so Surefire selects it when the profile is
  active. It self-skips (does not fail) when no Docker daemon is present.
- **Test profile.** All Spring tests use `@ActiveProfiles("test")` -> `application-test.yml`
  (H2 in-memory, `ddl-auto=validate` against `schema.sql`, sample `data.sql` suppressed,
  `outbox.dispatcher.enabled=false`, Kafka bootstrap bound to the embedded broker). Because the
  scheduler is disabled, integration tests invoke `dispatchOnce()` / `purgeExpiredRows()` explicitly
  for determinism.
- **Fixtures.** Tests reuse the shared factories (`ApplicationTestDataFactory`, `AvroPayloadFactory`,
  `OutboxEventTestDataFactory`, `OutboxDlqTestDataFactory`) so payloads and entities stay consistent.
- **H2 FK constraint (GOTCHA #1).** The H2 `schema.sql` enforces
  `fk_outbox_app (outbox.application_id -> applications.application_id)`. Integration tests that insert
  outbox rows directly therefore persist a matching `Application` first (via
  `ApplicationTestDataFactory.validApplication()`) and align the outbox row's `applicationId` /
  `correlationId` to it.
- **`@Lob` payload + Postgres `create-drop` rationale.** `OutboxEvent.payload` is `@Lob byte[]`, and the
  H2 `schema.sql` is not Postgres-compatible. On Postgres the base class lets Hibernate own the schema
  (`ddl-auto=create-drop`, `spring.sql.init.mode=never`), so the entity mappings and column types stay
  consistent. A side effect: the Postgres schema has **no** FK to `applications`, so the concurrency
  test inserts outbox rows without matching application rows.
- **Backoff-reset technique (GOTCHA #3).** A failed row's `scheduledRetryAt` jumps 1s/2s/4s into the
  future. To drive a row to the DLQ quickly without sleeping, `OutboxDlqIntegrationTest` reloads the
  row and resets `scheduledRetryAt = now` before each `dispatchOnce()`, so all four failing cycles run
  back-to-back deterministically.
- **Forced publish failure.** `OutboxDlqIntegrationTest` uses a `@MockitoBean OutboxPublisher` that
  throws `OutboxPublishException`; `OutboxProcessor` swallows it (dispatch does not throw), so the
  failure drives the retry/DLQ state machine rather than aborting the dispatch transaction.
- **Async assertions.** Awaitility with bounded timeouts is used for the Kafka publish assertions; no
  `Thread.sleep`.

## Remaining Work

- **Postgres concurrency test requires Docker.** `OutboxRetrievalConcurrencyPostgresTest` is excluded
  from the default build. Run it with `mvn test -Ptestcontainers` on a machine with a running Docker
  daemon to exercise `FOR UPDATE SKIP LOCKED`. (It was executed and passed in the implementation
  environment against a `postgres:16-alpine` container; on a Docker-less machine it self-skips.)
- **Optional Testcontainers Kafka variant.** Messaging tests use `@EmbeddedKafka` by default; a
  Testcontainers Kafka variant for production parity remains optional and, if added, would be tagged
  `testcontainers` (opt-in) as well.
- **JaCoCo thresholds not enforced.** JaCoCo produces reports but does not fail the build on coverage
  (consistent with the strategy's current assumption of report-only). Enforcing per-package branch/line
  thresholds is a possible follow-up.
- **Avro schema-compatibility checks** in CI (Open Question #2 in the strategy) are not part of this
  suite and remain tracked separately.
