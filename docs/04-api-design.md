# 04 - API Design

## Overview

This document describes the REST API contract, DTOs, validation rules, controller responsibilities and service interface for the Application Submission API.

Kafka publishing is **not** handled in the REST layer. The service writes an outbox record in the same transaction as the application persist; a background outbox processor (`OutboxDispatcher` / `OutboxPublisher`) publishes to Kafka asynchronously.

## Implementation Status (as of 2026-07-07)

| Item | Status |
|------|--------|
| POST /applications | Implemented |
| Request/response DTOs + validation | Implemented |
| Global exception handler | Implemented |
| Transactional outbox write in service | Implemented |
| Kafka publish (via outbox processor) | Implemented |
| Admin DLQ / replay endpoints | Implemented |
| GET /applications/{applicationId} | Not implemented (deferred) |
| Application fallback replay REST endpoint | Not implemented (service method only) |

## Endpoints

### POST /applications

- Description: Submit a new application.
- Request: JSON body containing applicant data (see Request DTO below).
- Response: 201 Created with JSON body containing `applicationId`, `correlationId`, `timestamp`, and `status`.
- Errors: 400 for validation errors, 500 for server errors.

### GET /applications/{applicationId}

- Description: Retrieve application by id.
- Status: **Deferred** — not implemented. Recommended for future completeness.

## Admin endpoints

Implemented in `OutboxAdminController` under `/admin`. These endpoints are part of the event-system tooling scope, separate from the submission API.

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| GET | `/admin/outbox-dlq` | List DLQ items (paginated) | Paginated `OutboxDlqResponse` |
| GET | `/admin/outbox-dlq/{id}` | DLQ item detail | `OutboxDlqResponse` |
| POST | `/admin/outbox-dlq/{id}/replay` | Trigger replay from DLQ payload | `ReplayResponse` |
| POST | `/admin/outbox/{id}/replay` | Trigger replay from outbox row | `ReplayResponse` |

### Admin DTOs

**`OutboxDlqResponse`** (record):

- `id`, `originalOutboxId`, `applicationId`, `correlationId`, `failureReason`, `attempts`, `failedAt`, `createdAt`

**`ReplayResponse`** (record):

- `message` (string), `success` (boolean)

Application-table fallback replay (`OutboxReplayService.replayFromApplication()`) is implemented as a service method but has no REST endpoint yet.

## Request / Response DTOs

### Request DTO: `ApplicationRequest`

- Fields:
  - firstName (string) — required, max 50, allowed characters: Unicode letters, spaces, hyphens, apostrophes
  - lastName (string) — required, max 50, same character rules
- Validation:
  - @NotBlank
  - @Size(max = 50)
  - @Pattern(regexp = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$")

### Response DTO: `ApplicationResponse`

- Fields:
  - applicationId (string, UUID)
  - correlationId (string, UUID)
  - timestamp (string, ISO-8601 UTC via `Instant.toString()`)
  - status (string, e.g. `"SUBMITTED"`)

Example response:

```json
{
  "applicationId": "...",
  "correlationId": "...",
  "timestamp": "2026-06-06T12:00:00Z",
  "status": "SUBMITTED"
}
```

## Controller design

- Package: `com.example.event_driven_design_demo.controller`
- Controller: `ApplicationController`
  - Thin controller: validate input and delegate to `ApplicationService`.
  - Single endpoint `POST /applications`.
  - Returns 201 Created on success.
  - Uses `@Validated` and `@Valid` for request validation.
- Controller: `OutboxAdminController`
  - Admin endpoints under `/admin` for DLQ listing and replay (see Admin endpoints above).

## Service layer design

- Package: `com.example.event_driven_design_demo.service`
- Service interface: `ApplicationService`
  - Method: `ApplicationResponse submitApplication(ApplicationRequest request)`
- Implementation: `ApplicationServiceImpl`
  - Generates `applicationId` and `correlationId` (UUID), `timestamp` (`Instant.now()`).
  - Persists `Application` entity via `ApplicationRepository`.
  - Serializes an `ApplicationSubmitted` Avro event and persists an `Outbox` row with `status = PENDING` in the **same transaction** via `OutboxRepository`.
  - Returns `ApplicationResponse` with string-formatted IDs and timestamp.
  - Does **not** publish to Kafka directly; that is the outbox processor's responsibility.
  - Uses constructor injection for repositories.

## Exception handling

- Global exception handler `@RestControllerAdvice` (package: `com.example.event_driven_design_demo.exception`)
  - Validation errors (`MethodArgumentNotValidException`, `ConstraintViolationException`) → HTTP 400 with `{ "errorCode": "VALIDATION_ERROR", "message": "..." }`.
  - `OutboxReplayException` (admin replay, missing DLQ/outbox row) → HTTP 404 with `{ "errorCode": "NOT_FOUND", "message": "..." }`.
  - Unexpected exceptions → HTTP 500 with `{ "errorCode": "INTERNAL_ERROR", "message": "..." }`.

## Decisions

- `GET /applications/{id}` is deferred.
- Timestamps use ISO-8601 UTC via `Instant.toString()` (e.g. `2026-06-06T12:00:00.123456789Z`).

## Implementation notes

- Use Jakarta Validation annotations on DTOs.
- Use Lombok `@Slf4j` for logging in services and controllers (no manual `LoggerFactory.getLogger`); avoid System.out.println.
- Follow constructor injection and layered architecture: controller → service → repository.
- Keep controllers thin; place business logic in services.
- springdoc-openapi is on the classpath; Swagger UI is available at `/swagger-ui.html` when the app is running.
