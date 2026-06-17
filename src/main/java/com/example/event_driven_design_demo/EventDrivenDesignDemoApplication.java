package com.example.event_driven_design_demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class EventDrivenDesignDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventDrivenDesignDemoApplication.class, args);
	}

}
