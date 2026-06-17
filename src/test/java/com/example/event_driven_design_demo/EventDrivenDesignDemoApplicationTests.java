package com.example.event_driven_design_demo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "outbox.dispatcher.enabled=false")
class EventDrivenDesignDemoApplicationTests {

	@Test
	void contextLoads() {
	}

}
