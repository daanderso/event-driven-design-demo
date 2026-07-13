package com.example.event_driven_design_demo.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Applicant data submitted to create a new application")
public class ApplicationRequest {

    @NotBlank
    @Size(max = 50)
    @Pattern(regexp = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$", message = "firstName contains invalid characters")
    @Schema(
            description = "Applicant first name",
            example = "Jane",
            maxLength = 50,
            pattern = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$")
    private String firstName;

    @NotBlank
    @Size(max = 50)
    @Pattern(regexp = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$", message = "lastName contains invalid characters")
    @Schema(
            description = "Applicant last name",
            example = "Doe",
            maxLength = 50,
            pattern = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$")
    private String lastName;

}


