package com.md5cracker.service;

import com.md5cracker.config.RabbitMQConfig;
import com.md5cracker.dto.HashBatch;
import com.md5cracker.dto.ResultBatch;
import com.md5cracker.repository.JobRepository;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class JobService {
    
    private static final int BATCH_SIZE = 1000;
    
    private final RabbitTemplate rabbitTemplate;
    private final JobRepository jobRepository;
    private final JdbcTemplate jdbcTemplate;
    private final JobEventPublisher eventPublisher;

    public JobService(RabbitTemplate rabbitTemplate, JobRepository jobRepository, 
                     JdbcTemplate jdbcTemplate, JobEventPublisher eventPublisher) {
        this.rabbitTemplate = rabbitTemplate;
        this.jobRepository = jobRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.eventPublisher = eventPublisher;
    }

    public UUID createJob(MultipartFile file) throws IOException {
        UUID jobId = UUID.randomUUID();
        
        // Read and filter hashes
        List<String> hashes;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            hashes = reader.lines()
                    .map(String::trim)
                    .filter(line -> !line.isBlank() && line.length() == 32)
                    .collect(Collectors.toList());
        }
        
        // Calculate batch count
        int batchCount = (hashes.size() + BATCH_SIZE - 1) / BATCH_SIZE; // Ceiling division
        
        // Create job record FIRST (required for foreign key constraint)
        jobRepository.createJob(jobId, hashes.size(), batchCount);
        
        // Store targets (now job exists)
        storeTargets(jobId, hashes);
        
        // Split into batches and publish
        int batchIndex = 0;
        for (int i = 0; i < hashes.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, hashes.size());
            List<String> batch = hashes.subList(i, end);
            
            HashBatch hashBatch = new HashBatch(jobId, batchIndex++, batch);
            rabbitTemplate.convertAndSend(
                RabbitMQConfig.EXCHANGE_NAME,
                RabbitMQConfig.LOOKUP_ROUTING_KEY,
                hashBatch
            );
        }
        
        return jobId;
    }

    protected void storeTargets(UUID jobId, List<String> hashes) {
        String sql = "INSERT INTO targets (job_id, hash_hex) VALUES (?, ?) ON CONFLICT DO NOTHING";
        List<Object[]> batchArgs = hashes.stream()
                .map(hash -> new Object[]{jobId, hash})
                .collect(Collectors.toList());
        jdbcTemplate.batchUpdate(sql, batchArgs);
    }

    public String generateResultsCsv(UUID jobId) {
        List<ResultRow> results = jdbcTemplate.query(
            "SELECT t.hash_hex, COALESCE(r.phone_number, 'NOT FOUND') as phone " +
            "FROM targets t " +
            "LEFT JOIN results r ON r.job_id = t.job_id AND r.hash_hex = t.hash_hex " +
            "WHERE t.job_id = ? " +
            "ORDER BY t.hash_hex",
            (rs, rowNum) -> new ResultRow(rs.getString("hash_hex"), rs.getString("phone")),
            jobId
        );
        
        StringBuilder csv = new StringBuilder("hash,phone\n");
        for (ResultRow row : results) {
            csv.append(row.hash).append(",").append(row.phone).append("\n");
        }
        return csv.toString();
    }

    public void handleResultBatch(ResultBatch resultBatch) {
        UUID jobId = resultBatch.jobId();
        int foundCount = resultBatch.results().size();
        
        // Update job progress
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        if (status == null) {
            // Job doesn't exist, ignore this result batch
            return;
        }
        
        int newBatchesCompleted = status.batchesCompleted() + 1;
        int newFoundCount = status.foundCount() + foundCount;
        
        jobRepository.updateJobProgress(jobId, newBatchesCompleted, newFoundCount);
        
        // Publish progress event
        eventPublisher.publishProgress(jobId, "progress", Map.of(
            "batchesCompleted", newBatchesCompleted,
            "batchesExpected", status.batchesExpected(),
            "foundCount", newFoundCount
        ));
        
        // Check if job is complete
        if (newBatchesCompleted >= status.batchesExpected()) {
            jobRepository.markJobComplete(jobId);
            eventPublisher.publishProgress(jobId, "completed", Map.of("jobId", jobId.toString()));
            eventPublisher.complete(jobId);
        }
    }

    private record ResultRow(String hash, String phone) {}
}

