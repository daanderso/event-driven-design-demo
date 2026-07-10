# 06 - Resilience Design (DB Call Resilience)

## Overview

This document specifies **DB-call resilience** for the Application Submission service: retry with exponential backoff and a circuit breaker around production database operations, and a fail-fast **HTTP 503** when the DB circuit is open. It is derived from the Approved Decisions in `docs/01-requirements.md` and `docs/05-kafka-outbox-design.md`, and it does **not** change the system architecture — it only wraps existing DB operations.

### Why this is needed

`ApplicationServiceImpl.submitApplication` and the scheduled DB jobs had no retry, so a transient Postgres blip surfaced to the client as a `500`. The outbox already protects *event publishing* on DB recovery, but the synchronous submit path and scheduled DB access were unprotected. This design adds retry + circuit breaking to those synchronous/scheduled DB calls.

### Scope

- **IN:** DB-call resilience (retry + exponential backoff + circuit breaker) on production DB operations; fail-fast `503` when the DB circuit is open.
- **OUT — Kafka outbox publishing:** Resilience4j is **not** applied to Kafka publishing. Kafka failures keep using the existing outbox retry/backoff/DLQ path in `OutboxProcessor` / `OutboxRetryPolicy` (see `docs/05-kafka-outbox-design.md`). The PENDING outbox rows are the durable replay buffer.
- **OUT — external outbound REST APIs:** there are no outbound HTTP calls today. A naming pattern is reserved below for the future, but nothing is implemented.

## Resilience strategy

### Instance naming convention: `{bounded-context}-{dependency}`

We deliberately **do not** name the instance `db`. `db` names the *technology*, not the failure domain or bounded context, and it does not scale once you add outbound HTTP clients, read replicas, or per-operation tuning.

A name should read like a sentence describing the guarded failure domain — e.g. *"resilience for persisting an application submission"* → `application-submission-persistence` — not abstract verbs like `write`.

| Instance name | Reads as | Scope | Used by |
|---|---|---|---|
| `application-submission-persistence` | DB resilience when a client submits an application (`POST /applications` → `applications` + `outbox` rows in one transaction) | User-facing submission path | `ApplicationServiceImpl.submitApplication` |
| `outbox-persistence` | DB resilience for outbox event rows (dispatch, cleanup, replay lookups) | Background outbox jobs | `OutboxDispatchService`, `OutboxCleanupJob`, `OutboxReplayReader` |

Names are centralized in `ResilienceInstanceNames` so annotations, YAML, tests, and docs stay in sync (a mismatched name silently falls back to Resilience4j defaults).

### Why two instances instead of one shared `db`

- **Isolation** — a sustained outage during background dispatch should not necessarily open the circuit for synchronous API submits (and vice versa).
- **Independent tuning** — user-facing writes need a shorter `wait-duration-in-open-state` (fail fast for callers); background jobs can tolerate longer backoff and more attempts.
- **Extensibility** — future instances slot in without renaming existing ones.

### Future instances (documented, not implemented)

- `{service-name}-client` — outbound HTTP to another production service (retry + circuit breaker + timeout), e.g. `verification-service-client`.
- `{read-model}-persistence` — if a read replica with different latency/retry needs is added.

## Configuration

Resilience4j supports `configs` (shared templates) and `instances` (named deployments that inherit a template and override only what differs). We define one shared `persistence-default` template per concern and two instances.

```yaml
resilience4j:
  retry:
    configs:
      persistence-default:
        max-attempts: 4
        wait-duration: 200ms
        enable-exponential-backoff: true
        exponential-backoff-multiplier: 2
        retry-exceptions:
          - org.springframework.dao.TransientDataAccessException
          - org.springframework.dao.RecoverableDataAccessException
          - org.springframework.transaction.CannotCreateTransactionException
          - jakarta.persistence.PessimisticLockException
        ignore-exceptions:
          - org.springframework.dao.DataIntegrityViolationException
          - org.springframework.orm.ObjectOptimisticLockingFailureException
    instances:
      application-submission-persistence:
        base-config: persistence-default
      outbox-persistence:
        base-config: persistence-default
        max-attempts: 6            # background jobs tolerate more attempts

  circuitbreaker:
    configs:
      persistence-default:
        sliding-window-type: COUNT_BASED
        sliding-window-size: 10
        minimum-number-of-calls: 5
        failure-rate-threshold: 50
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        record-exceptions:
          - org.springframework.dao.DataAccessException
          - org.springframework.transaction.TransactionException
        ignore-exceptions:
          - org.springframework.dao.DataIntegrityViolationException
          - org.springframework.orm.ObjectOptimisticLockingFailureException
    instances:
      application-submission-persistence:
        base-config: persistence-default
        wait-duration-in-open-state: 5s    # fail fast for API callers
      outbox-persistence:
        base-config: persistence-default
        wait-duration-in-open-state: 30s   # background can wait longer

  # Aspect order: higher runs first (outermost). Retry wraps CircuitBreaker,
  # which wraps @Transactional (Spring default LOWEST_PRECEDENCE = innermost),
  # so each retry attempt runs in a fresh transaction.
  retry-aspect-order: 3
  circuitbreaker-aspect-order: 2

management:
  health:
    circuitbreakers:
      enabled: true   # exposes both instances under /actuator/health
  endpoint:
    health:
      show-details: always
```

### Adding a new instance later

1. Add one constant to `ResilienceInstanceNames` (e.g. `VERIFICATION_SERVICE_CLIENT = "verification-service-client"`).
2. Add one `instances` entry under `retry` and `circuitbreaker` with `base-config: persistence-default` (or a new template), overriding only what differs.
3. Annotate the guarded method with the constant.

No existing instance changes.

### Exception handling rationale

- **Retry only transient/recoverable faults:** connection loss (`TransientDataAccessException` / `RecoverableDataAccessException`), cannot-create-transaction (`CannotCreateTransactionException`), and pessimistic locks (`PessimisticLockException`). Never retry permanent faults — `DataIntegrityViolationException` (constraint violations) and `ObjectOptimisticLockingFailureException` (optimistic-lock) are in `ignore-exceptions`.
- **Circuit breaker records** broad DB/transaction failures (`DataAccessException`, `TransactionException`) but ignores the same permanent faults, so a client sending bad data cannot trip the breaker for everyone else.
- **Aspect nesting:** Retry (outermost) → CircuitBreaker → `@Transactional` (innermost). Because Spring's `@Transactional` runs at `LOWEST_PRECEDENCE`, each retry attempt executes in a **fresh transaction**, which is required for a retry to have any chance of succeeding after a rolled-back attempt. The explicit `retry-aspect-order`/`circuitbreaker-aspect-order` make this guaranteed and reviewable.

## Code examples

Instance names come from the shared constants class, never string literals at the call site.

```12:20:src/main/java/com/example/event_driven_design_demo/resilience/ResilienceInstanceNames.java
public final class ResilienceInstanceNames {
    public static final String APPLICATION_SUBMISSION_PERSISTENCE = "application-submission-persistence";
    public static final String OUTBOX_PERSISTENCE = "outbox-persistence";
}
```

Synchronous submit path (fail-fast for API callers):

```java
@Retry(name = ResilienceInstanceNames.APPLICATION_SUBMISSION_PERSISTENCE)
@CircuitBreaker(name = ResilienceInstanceNames.APPLICATION_SUBMISSION_PERSISTENCE)
@Transactional
public ApplicationResponse submitApplication(ApplicationRequest request) { ... }
```

Background outbox DB ops:

```java
@Retry(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
@CircuitBreaker(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
@Transactional
public int dispatchOnce() { ... }
```

Replay DB **reads** are isolated in `OutboxReplayReader` (a separate bean) so the aspects are applied through the Spring proxy — self-invocation from `OutboxReplayService` would bypass them. Only the DB lookup is guarded; the subsequent Kafka publish stays in `OutboxReplayService`:

```java
@Retry(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
@CircuitBreaker(name = ResilienceInstanceNames.OUTBOX_PERSISTENCE)
public OutboxEvent findOutboxEvent(Long outboxId) {
    return outboxRepository.findById(outboxId)
            .orElseThrow(() -> new OutboxReplayException("Outbox row not found: " + outboxId));
}
```

Fail-fast HTTP mapping (`GlobalExceptionHandler`): a `CallNotPermittedException` (circuit open) or a `TransientDataAccessException`/`DataAccessResourceFailureException` that survived all retries is mapped to **HTTP 503** with error code `SERVICE_UNAVAILABLE`.

```java
@ExceptionHandler(CallNotPermittedException.class)
protected ResponseEntity<ApiError> handleCircuitOpen(CallNotPermittedException ex) {
    ApiError error = new ApiError("SERVICE_UNAVAILABLE", "Service temporarily unavailable, please retry shortly");
    return new ResponseEntity<>(error, HttpStatus.SERVICE_UNAVAILABLE);
}
```

## Failure scenarios

| Scenario | `application-submission-persistence` | `outbox-persistence` |
|---|---|---|
| Single transient DB blip | Retried (up to 4 attempts, 200ms × 2 backoff); succeeds transparently | Retried (up to 6 attempts); succeeds transparently |
| Permanent fault (constraint / optimistic lock) | Not retried, not recorded by CB; surfaces normally (e.g. validation/500) | Same |
| Sustained DB outage — circuit **opens** | Opens after failure rate ≥ 50% over ≥ 5 calls; `POST /applications` returns **503** fast (open state 5s) | Opens independently; scheduled dispatcher/cleanup skip their cycle (already wrapped in try/catch) and resume when DB recovers (open state 30s) |
| Recovery | After 5s the breaker half-opens, permits 3 trial calls, and closes if they succeed | After 30s half-opens the same way; PENDING outbox rows are replayed on the next successful cycle — no events lost |
| Isolation | An open submit circuit does **not** block background dispatch | An open dispatch circuit does **not** block synchronous submits |

Circuit state is observable at `GET /actuator/health` (both instances shown via `management.health.circuitbreakers.enabled: true`).

## Maven dependencies

Three related artifacts are declared in `pom.xml`:

| Artifact | Purpose |
|---|---|
| `spring-cloud-starter-circuitbreaker-reactor-resilience4j` | Brings in the Resilience4j core modules transitively (circuit breaker, retry, reactor adapters) |
| `resilience4j-spring-boot3` | Enables `@Retry` / `@CircuitBreaker` annotation support via AOP |
| `spring-boot-starter-aspectj` | Required so the annotation aspects can weave (Spring Boot 4 renamed `spring-boot-starter-aop` → `-aspectj`) |

**Version management:** Do **not** pin an explicit `<version>` on `resilience4j-spring-boot3`. It is managed by the `spring-cloud-dependencies` BOM (which imports the Resilience4j BOM), keeping it aligned with the `resilience4j-core` / `-circuitbreaker` versions pulled in transitively by `spring-cloud-starter-circuitbreaker-reactor-resilience4j`. A hardcoded version can drift from the BOM and produce a split-version classpath (e.g. `NoSuchMethodError` at runtime). Verify the resolved tree with:

```bash
mvn dependency:tree -Dincludes=io.github.resilience4j
```

**Fallback:** If `resilience4j-spring-boot3` is ever incompatible with the Spring Boot version, drop it and `spring-boot-starter-aspectj` and switch to programmatic `RetryRegistry` / `CircuitBreakerRegistry` beans behind a small `DbResilience.execute(...)` helper (same behavior, no AOP).

## Implementation status (as of 2026-07-07)

| Component | Status |
|---|---|
| `resilience4j-spring-boot3` + AspectJ starter on classpath | Implemented (`pom.xml`) |
| Shared `persistence-default` configs + two instances | Implemented (`application.yml`) |
| `ResilienceInstanceNames` constants | Implemented |
| `@Retry`/`@CircuitBreaker` on `submitApplication` | Implemented |
| `@Retry`/`@CircuitBreaker` on dispatch/cleanup/replay-read | Implemented |
| `503` mapping for circuit-open + transient DB failures | Implemented (`GlobalExceptionHandler`) |
| Actuator circuit-breaker health | Implemented |
| Focused tests (retry, circuit-open → 503, isolation) | Implemented |

## Open questions

- **Extending the circuit breaker to more DB calls:** stakeholders may want read-path or admin endpoints guarded too. Currently only the submit path and outbox jobs are wrapped. Revisit once metrics show where transient failures actually surface.
- **External-API instance naming reserved:** `{service-name}-client` (e.g. `verification-service-client`) is reserved for future outbound HTTP resilience (retry + CB + timeout). Not implemented — there are no outbound calls today.
- **Per-instance tuning values** (`max-attempts`, `wait-duration-in-open-state`) are initial estimates and should be tuned against production latency/error telemetry.

## References

- `docs/01-requirements.md` — requirements and Approved Decisions (authoritative)
- `docs/05-kafka-outbox-design.md` — Kafka/outbox retry path (excluded from Resilience4j)
- `ResilienceInstanceNames` — single source of truth for instance names
- `application.yml` — `resilience4j` block
