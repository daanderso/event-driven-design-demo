package com.example.event_driven_design_demo.repository;

import com.example.event_driven_design_demo.entity.Application;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ApplicationRepository extends JpaRepository<Application, UUID> {
}

