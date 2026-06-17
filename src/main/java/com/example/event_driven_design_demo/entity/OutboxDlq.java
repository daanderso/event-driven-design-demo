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
@Table(name = "outbox_dlq")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OutboxDlq {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "original_outbox_id")
    private Long originalOutboxId;

    @Column(name = "application_id")
    private UUID applicationId;

    @Column(name = "correlation_id")
    private UUID correlationId;

    @Lob
    @Column(name = "payload")
    private byte[] payload;

    @Lob
    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "attempts")
    private Integer attempts;

    @Column(name = "failed_at", nullable = false)
    private Instant failedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

}




