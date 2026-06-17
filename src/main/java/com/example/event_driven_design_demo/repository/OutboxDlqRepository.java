package com.example.event_driven_design_demo.repository;

import java.time.Instant;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.example.event_driven_design_demo.entity.OutboxDlq;


public interface OutboxDlqRepository extends JpaRepository<OutboxDlq, Long> {

    @Modifying
    @Query("DELETE FROM OutboxDlq d WHERE d.failedAt < :cutoff")
    int deleteFailedBefore(@Param("cutoff") Instant cutoff);
}
