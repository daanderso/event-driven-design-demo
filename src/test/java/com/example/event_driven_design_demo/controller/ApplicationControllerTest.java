package com.example.event_driven_design_demo.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.example.event_driven_design_demo.dto.ApplicationRequest;
import com.example.event_driven_design_demo.dto.ApplicationResponse;
import com.example.event_driven_design_demo.fixtures.ApplicationTestDataFactory;
import com.example.event_driven_design_demo.service.ApplicationService;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;

/**
 * API tests for the submission endpoint: HTTP contract, request validation, and error mapping.
 */
@WebMvcTest(ApplicationController.class)
class ApplicationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @MockitoBean
    private ApplicationService applicationService;

    @Test
    void submit_validRequest_returns201WithBody() throws Exception {
        when(applicationService.submitApplication(any(ApplicationRequest.class)))
                .thenReturn(new ApplicationResponse(
                        "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                        "9b2c4f1e-8d3a-4b5c-9e6f-1234567890ab",
                        "2026-07-13T12:00:00Z",
                        "SUBMITTED"));

        mockMvc.perform(post("/applications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(ApplicationTestDataFactory.validRequest())))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.applicationId").value("3fa85f64-5717-4562-b3fc-2c963f66afa6"))
                .andExpect(jsonPath("$.correlationId").value("9b2c4f1e-8d3a-4b5c-9e6f-1234567890ab"))
                .andExpect(jsonPath("$.status").value("SUBMITTED"));
    }

    @Test
    void submit_blankFirstName_returns400ValidationError() throws Exception {
        mockMvc.perform(post("/applications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(ApplicationTestDataFactory.requestWith("", "Doe"))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"));
    }

    @Test
    void submit_tooLongLastName_returns400ValidationError() throws Exception {
        mockMvc.perform(post("/applications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                ApplicationTestDataFactory.requestWith("Jane", ApplicationTestDataFactory.tooLongName()))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"));
    }

    @Test
    void submit_regexViolatingName_returns400ValidationError() throws Exception {
        mockMvc.perform(post("/applications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(ApplicationTestDataFactory.requestWith("Jane123", "Doe"))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"));
    }

    @Test
    void submit_whenCircuitOpen_returns503ServiceUnavailable() throws Exception {
        when(applicationService.submitApplication(any(ApplicationRequest.class)))
                .thenThrow(mock(CallNotPermittedException.class));

        mockMvc.perform(post("/applications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(ApplicationTestDataFactory.validRequest())))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.errorCode").value("SERVICE_UNAVAILABLE"));
    }

    @Test
    void submit_whenTransientDbFailure_returns503ServiceUnavailable() throws Exception {
        when(applicationService.submitApplication(any(ApplicationRequest.class)))
                .thenThrow(new TransientDataAccessResourceException("db down"));

        mockMvc.perform(post("/applications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(ApplicationTestDataFactory.validRequest())))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.errorCode").value("SERVICE_UNAVAILABLE"));
    }
}
