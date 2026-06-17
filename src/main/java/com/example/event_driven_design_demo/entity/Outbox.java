package com.example.event_driven_design_demo.entity;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "outbox")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Outbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "application_id", nullable = false)
    private UUID applicationId;

    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    @Lob
    @Column(name = "payload", nullable = false)
    private byte[] payload;

    @Column(name = "content_type")
    private String contentType;

    @Column(name = "status", length = 16, nullable = false)
    private String status;

    @Column(name = "attempts", nullable = false)
    private Integer attempts = 0;

    @Lob
    @Column(name = "last_error")
    private String lastError;

    @Column(name = "scheduled_retry_at", nullable = false)
    private Instant scheduledRetryAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

}




