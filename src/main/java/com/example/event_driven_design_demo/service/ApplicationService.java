package com.example.event_driven_design_demo.service;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;

public interface ApplicationService {

    ApplicationResponse submitApplication(ApplicationRequest request);

}

