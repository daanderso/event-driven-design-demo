package com.example.event_driven_design_demo.repository;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.example.event_driven_design_demo.entity.OutboxEvent;


public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    @Query("SELECT o FROM Outbox o WHERE o.status = 'PENDING' AND o.scheduledRetryAt <= :now ORDER BY o.id")
    List<OutboxEvent> findPending(@Param("now") Instant now);

    @Query("SELECT o FROM Outbox o WHERE o.status = 'PENDING' AND o.scheduledRetryAt <= :now ORDER BY o.id")
    List<OutboxEvent> findPending(@Param("now") Instant now, Pageable pageable);

    @Modifying
    @Query("DELETE FROM Outbox o WHERE o.status = 'PUBLISHED' AND o.publishedAt < :cutoff")
    int deletePublishedBefore(@Param("cutoff") Instant cutoff);
}
