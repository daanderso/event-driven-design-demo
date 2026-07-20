package com.example.event_driven_design_demo.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.example.event_driven_design_demo.entity.OutboxDlq;
import com.example.event_driven_design_demo.fixtures.OutboxDlqTestDataFactory;
import com.example.event_driven_design_demo.outbox.replay.OutboxReplayException;
import com.example.event_driven_design_demo.outbox.replay.OutboxReplayService;
import com.example.event_driven_design_demo.repository.OutboxDlqRepository;

/**
 * API tests for the operator endpoints: DLQ listing/detail (incl. 404) and replay triggering.
 */
@WebMvcTest(OutboxAdminController.class)
class OutboxAdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OutboxDlqRepository outboxDlqRepository;

    @MockitoBean
    private OutboxReplayService replayService;

    @Test
    void listDlqItems_returnsPagedResponses() throws Exception {
        OutboxDlq dlq = OutboxDlqTestDataFactory.dlq();
        dlq.setId(1L);
        Page<OutboxDlq> page = new PageImpl<>(List.of(dlq));
        when(outboxDlqRepository.findAll(any(Pageable.class))).thenReturn(page);

        mockMvc.perform(get("/admin/outbox-dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].id").value(1));
    }

    @Test
    void getDlqItem_whenPresent_returns200() throws Exception {
        OutboxDlq dlq = OutboxDlqTestDataFactory.dlq();
        dlq.setId(7L);
        when(outboxDlqRepository.findById(7L)).thenReturn(Optional.of(dlq));

        mockMvc.perform(get("/admin/outbox-dlq/7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(7));
    }

    @Test
    void getDlqItem_whenAbsent_returns404NotFound() throws Exception {
        when(outboxDlqRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/admin/outbox-dlq/99"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("NOT_FOUND"));
    }

    @Test
    void replayDlq_returnsSuccessResponse() throws Exception {
        mockMvc.perform(post("/admin/outbox-dlq/5/replay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        verify(replayService).replayDlq(5L);
    }

    @Test
    void replayOutbox_returnsSuccessResponse() throws Exception {
        mockMvc.perform(post("/admin/outbox/42/replay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        verify(replayService).replayOutbox(42L);
    }

    @Test
    void replayOutbox_whenIneligible_returns404NotFound() throws Exception {
        doThrow(new OutboxReplayException("Outbox row is not eligible for replay: status=PENDING"))
                .when(replayService).replayOutbox(eq(3L));

        mockMvc.perform(post("/admin/outbox/3/replay"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("NOT_FOUND"));
    }
}
