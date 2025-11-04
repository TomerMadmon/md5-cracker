package com.md5cracker.controller;

import com.md5cracker.repository.JobRepository;
import com.md5cracker.service.JobEventPublisher;
import com.md5cracker.service.JobService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
    
    private final JobService jobService;
    private final JobRepository jobRepository;
    private final JobEventPublisher eventPublisher;

    public JobController(JobService jobService, JobRepository jobRepository, JobEventPublisher eventPublisher) {
        this.jobService = jobService;
        this.jobRepository = jobRepository;
        this.eventPublisher = eventPublisher;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> upload(@RequestParam("file") MultipartFile file) throws IOException {
        UUID jobId = jobService.createJob(file);
        eventPublisher.publishProgress(jobId, "job_created", Map.of("jobId", jobId.toString()));
        return ResponseEntity.accepted().body(Map.of("jobId", jobId.toString()));
    }

    @GetMapping
    public ResponseEntity<List<JobRepository.JobStatus>> listJobs() {
        List<JobRepository.JobStatus> jobs = jobRepository.listCompletedJobs();
        return ResponseEntity.ok(jobs);
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<?> getJob(@PathVariable UUID jobId) {
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        if (status == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(status);
    }

    @GetMapping(value = "/{jobId}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getEvents(@PathVariable UUID jobId) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        eventPublisher.registerEmitter(jobId, emitter);
        return emitter;
    }

    @GetMapping("/{jobId}/results")
    public ResponseEntity<?> getResults(@PathVariable UUID jobId) {
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        if (status == null) {
            return ResponseEntity.notFound().build();
        }
        
        String csv = jobService.generateResultsCsv(jobId);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + jobId + "-results.csv\"")
                .contentType(MediaType.TEXT_PLAIN)
                .body(csv);
    }
}

