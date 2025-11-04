package com.md5cracker.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class JobRepository {
    
    private final JdbcTemplate jdbcTemplate;

    public JobRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createJob(UUID jobId, int totalHashes, int batchesExpected) {
        jdbcTemplate.update(
            "INSERT INTO jobs (job_id, total_hashes, batches_expected, status) VALUES (?, ?, ?, 'RUNNING')",
            jobId, totalHashes, batchesExpected
        );
    }

    public void updateJobProgress(UUID jobId, int batchesCompleted, int foundCount) {
        jdbcTemplate.update(
            "UPDATE jobs SET batches_completed = ?, found_count = ? WHERE job_id = ?",
            batchesCompleted, foundCount, jobId
        );
    }

    public void markJobComplete(UUID jobId) {
        jdbcTemplate.update(
            "UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?",
            jobId
        );
    }

    public JobStatus getJobStatus(UUID jobId) {
        List<JobStatus> results = jdbcTemplate.query(
            "SELECT job_id, created_at, status, total_hashes, batches_expected, batches_completed, found_count " +
            "FROM jobs WHERE job_id = ?",
            (rs, rowNum) -> new JobStatus(
                UUID.fromString(rs.getString("job_id")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getString("status"),
                rs.getInt("total_hashes"),
                rs.getInt("batches_expected"),
                rs.getInt("batches_completed"),
                rs.getInt("found_count")
            ),
            jobId
        );
        
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    public List<JobStatus> listCompletedJobs() {
        return jdbcTemplate.query(
            "SELECT job_id, created_at, status, total_hashes, batches_expected, batches_completed, found_count " +
            "FROM jobs WHERE status = 'COMPLETED' " +
            "ORDER BY created_at DESC",
            (rs, rowNum) -> new JobStatus(
                UUID.fromString(rs.getString("job_id")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getString("status"),
                rs.getInt("total_hashes"),
                rs.getInt("batches_expected"),
                rs.getInt("batches_completed"),
                rs.getInt("found_count")
            )
        );
    }

    public record JobStatus(
        UUID jobId,
        java.time.Instant createdAt,
        String status,
        int totalHashes,
        int batchesExpected,
        int batchesCompleted,
        int foundCount
    ) {}
}

