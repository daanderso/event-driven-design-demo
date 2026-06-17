-- Sample data for local development (H2). Inserts a couple of applications and outbox entries to exercise basic flows.

INSERT INTO applications (application_id, correlation_id, first_name, last_name, status, created_at, updated_at, version)
VALUES (RANDOM_UUID(), RANDOM_UUID(), 'Alice', 'Smith', 'SUBMITTED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0);

INSERT INTO applications (application_id, correlation_id, first_name, last_name, status, created_at, updated_at, version)
VALUES (RANDOM_UUID(), RANDOM_UUID(), 'Bob', 'Johnson', 'SUBMITTED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0);

-- Insert a PENDING outbox row for Alice (small binary placeholder payload)
INSERT INTO outbox (application_id, correlation_id, payload, content_type, status, attempts, scheduled_retry_at, created_at)
VALUES ((SELECT application_id FROM applications WHERE first_name = 'Alice' LIMIT 1), (SELECT correlation_id FROM applications WHERE first_name = 'Alice' LIMIT 1), X'01', 'avro/binary', 'PENDING', 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Insert a PUBLISHED outbox row for Bob (published_at set)
INSERT INTO outbox (application_id, correlation_id, payload, content_type, status, attempts, scheduled_retry_at, created_at, published_at)
VALUES ((SELECT application_id FROM applications WHERE first_name = 'Bob' LIMIT 1), (SELECT correlation_id FROM applications WHERE first_name = 'Bob' LIMIT 1), X'02', 'avro/binary', 'PUBLISHED', 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Insert a DLQ record for demonstration
INSERT INTO outbox_dlq (original_outbox_id, application_id, correlation_id, payload, failure_reason, attempts, failed_at, created_at)
VALUES (NULL, (SELECT application_id FROM applications WHERE first_name = 'Bob' LIMIT 1), (SELECT correlation_id FROM applications WHERE first_name = 'Bob' LIMIT 1), X'FF', 'Simulated permanent failure', 3, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

