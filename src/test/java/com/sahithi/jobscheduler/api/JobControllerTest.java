package com.sahithi.jobscheduler.api;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahithi.jobscheduler.repository.JobRepository;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void clearJobs() {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");
    }

    @Test
    @DisplayName("POST /api/jobs enqueues a job and returns 201 with its id")
    void enqueueReturnsCreated() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("""
                                {"jobType":"echo","payload":"{\\"k\\":\\"v\\"}","priority":5}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists());
    }

    @Test
    @DisplayName("POST with the same dedupeKey returns the original job's id rather than creating a second job")
    void enqueueIsIdempotentForTheSameDedupeKey() throws Exception {
        var body = """
                {"jobType":"echo","dedupeKey":"order-42"}
                """;

        var first = mockMvc.perform(post("/api/jobs").contentType("application/json").content(body))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();
        var second = mockMvc.perform(post("/api/jobs").contentType("application/json").content(body))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

        var firstId = objectMapper.readTree(first).get("id").asText();
        var secondId = objectMapper.readTree(second).get("id").asText();
        org.assertj.core.api.Assertions.assertThat(secondId).isEqualTo(firstId);

        var total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM jobs", Integer.class);
        org.assertj.core.api.Assertions.assertThat(total).isEqualTo(1);
    }

    @Test
    @DisplayName("POST without a jobType is rejected with 400 rather than creating an unrunnable job")
    void enqueueRejectsMissingJobType() throws Exception {
        mockMvc.perform(post("/api/jobs").contentType("application/json").content("""
                        {"payload":"{}"}
                        """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("GET /api/jobs/{id} returns the job, or 404 when it does not exist")
    void getByIdReturnsJobOrNotFound() throws Exception {
        var id = repository.enqueue("echo", "{}", 0, 3, null);

        mockMvc.perform(get("/api/jobs/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobType").value("echo"))
                .andExpect(jsonPath("$.status").value("PENDING"));

        mockMvc.perform(get("/api/jobs/" + UUID.randomUUID())).andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/jobs?status= filters by status")
    void listFiltersByStatus() throws Exception {
        repository.enqueue("echo", "{}", 0, 3, null);
        repository.enqueue("echo", "{}", 0, 3, null);
        repository.claimBatch("w1", 1); // one becomes RUNNING

        mockMvc.perform(get("/api/jobs").param("status", "PENDING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)));
        mockMvc.perform(get("/api/jobs").param("status", "RUNNING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)));
    }

    @Test
    @DisplayName("GET /api/jobs/stats reports queue depth by status")
    void statsReportsQueueDepth() throws Exception {
        repository.enqueue("echo", "{}", 0, 3, null);
        repository.enqueue("echo", "{}", 0, 3, null);
        repository.claimBatch("w1", 1);

        mockMvc.perform(get("/api/jobs/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.PENDING").value(1))
                .andExpect(jsonPath("$.RUNNING").value(1));
    }
}
