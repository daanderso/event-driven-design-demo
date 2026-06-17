package com.example.event_driven_design_demo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationResponse {

    private String applicationId;
    private String correlationId;
    private String timestamp;
    private String status;

}


