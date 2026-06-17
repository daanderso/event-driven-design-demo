package com.example.event_driven_design_demo.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationRequest {

    @NotBlank
    @Size(max = 50)
    @Pattern(regexp = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$", message = "firstName contains invalid characters")
    private String firstName;

    @NotBlank
    @Size(max = 50)
    @Pattern(regexp = "^[\\p{L}]+(?:[ '\\-][\\p{L}]+)*$", message = "lastName contains invalid characters")
    private String lastName;

}


