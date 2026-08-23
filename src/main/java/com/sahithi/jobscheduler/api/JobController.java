package com.sahithi.jobscheduler.api;

import com.sahithi.jobscheduler.domain.Job;
import com.sahithi.jobscheduler.domain.JobStatus;
import com.sahithi.jobscheduler.repository.JobRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobRepository repository;

    public JobController(JobRepository repository) {
        this.repository = repository;
    }

    public record EnqueueRequest(
            @NotBlank String jobType,
            String payload,
            int priority,
            @Min(1) Integer maxAttempts,
            String dedupeKey) {
    }

    public record EnqueueResponse(UUID id) {
    }

    @PostMapping
    public ResponseEntity<EnqueueResponse> enqueue(@Valid @RequestBody EnqueueRequest request) {
        var id = repository.enqueue(
                request.jobType(),
                request.payload() == null ? "{}" : request.payload(),
                request.priority(),
                request.maxAttempts() == null ? 5 : request.maxAttempts(),
                request.dedupeKey());
        return ResponseEntity.status(HttpStatus.CREATED).body(new EnqueueResponse(id));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Job> get(@PathVariable UUID id) {
        return repository.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<Job> listByStatus(
            @RequestParam JobStatus status, @RequestParam(defaultValue = "50") int limit) {
        return repository.findByStatus(status, Math.min(limit, 500));
    }

    /** Queue depth by status - the first thing you'd look at to see whether workers are keeping up. */
    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return repository.countByStatus();
    }
}
