-- Schema for local H2 development (will be executed by Spring Boot on startup when using default settings)
-- Creates applications, outbox and outbox_dlq tables compatible with H2 and PostgreSQL semantics where possible.

CREATE TABLE IF NOT EXISTS applications (
  application_id UUID PRIMARY KEY,
  correlation_id UUID NOT NULL,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  status VARCHAR(32) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  version BIGINT
);
CREATE INDEX IF NOT EXISTS idx_applications_correlation_id ON applications(correlation_id);

CREATE TABLE IF NOT EXISTS outbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  application_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  payload BLOB NOT NULL,
  content_type VARCHAR(64),
  status VARCHAR(16) NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  last_error CLOB,
  scheduled_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  published_at TIMESTAMP NULL,
  CONSTRAINT fk_outbox_app FOREIGN KEY (application_id) REFERENCES applications(application_id)
);
CREATE INDEX IF NOT EXISTS idx_outbox_status_retry ON outbox(status, scheduled_retry_at);
CREATE INDEX IF NOT EXISTS idx_outbox_application_id ON outbox(application_id);
CREATE INDEX IF NOT EXISTS idx_outbox_correlation_id ON outbox(correlation_id);

CREATE TABLE IF NOT EXISTS outbox_dlq (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  original_outbox_id BIGINT,
  application_id UUID,
  correlation_id UUID,
  payload BLOB,
  failure_reason CLOB,
  attempts INT,
  failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_outbox_dlq_failed_at ON outbox_dlq(failed_at);
CREATE INDEX IF NOT EXISTS idx_outbox_dlq_application_id ON outbox_dlq(application_id);
CREATE INDEX IF NOT EXISTS idx_outbox_dlq_correlation_id ON outbox_dlq(correlation_id);

