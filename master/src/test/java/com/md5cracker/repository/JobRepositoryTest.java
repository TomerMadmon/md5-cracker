package com.md5cracker.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private JobRepository jobRepository;

    private UUID jobId;
    private Instant createdAt;

    @BeforeEach
    void setUp() {
        jobId = UUID.randomUUID();
        createdAt = Instant.now();
    }

    @Test
    void createJob_ValidParameters_InsertsJob() {
        // Arrange
        int totalHashes = 1000;
        int batchesExpected = 10;
        
        when(jdbcTemplate.update(anyString(), eq(jobId), eq(totalHashes), eq(batchesExpected)))
            .thenReturn(1);

        // Act
        jobRepository.createJob(jobId, totalHashes, batchesExpected);

        // Assert
        verify(jdbcTemplate, times(1)).update(
            eq("INSERT INTO jobs (job_id, total_hashes, batches_expected, status) VALUES (?, ?, ?, 'RUNNING')"),
            eq(jobId),
            eq(totalHashes),
            eq(batchesExpected)
        );
    }

    @Test
    void updateJobProgress_ValidParameters_UpdatesJob() {
        // Arrange
        int batchesCompleted = 5;
        int foundCount = 25;
        
        when(jdbcTemplate.update(anyString(), eq(batchesCompleted), eq(foundCount), eq(jobId)))
            .thenReturn(1);

        // Act
        jobRepository.updateJobProgress(jobId, batchesCompleted, foundCount);

        // Assert
        verify(jdbcTemplate, times(1)).update(
            eq("UPDATE jobs SET batches_completed = ?, found_count = ? WHERE job_id = ?"),
            eq(batchesCompleted),
            eq(foundCount),
            eq(jobId)
        );
    }

    @Test
    void markJobComplete_ValidJobId_UpdatesStatus() {
        // Arrange
        when(jdbcTemplate.update(anyString(), eq(jobId))).thenReturn(1);

        // Act
        jobRepository.markJobComplete(jobId);

        // Assert
        verify(jdbcTemplate, times(1)).update(
            eq("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?"),
            eq(jobId)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void getJobStatus_ExistingJob_ReturnsJobStatus() {
        // Arrange
        JobRepository.JobStatus expectedStatus = new JobRepository.JobStatus(
            jobId,
            createdAt,
            "RUNNING",
            1000,
            10,
            5,
            25
        );

        when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq(jobId)))
            .thenReturn((List<JobRepository.JobStatus>) Arrays.asList(expectedStatus));

        // Act
        JobRepository.JobStatus result = jobRepository.getJobStatus(jobId);

        // Assert
        assertNotNull(result);
        assertEquals(jobId, result.jobId());
        assertEquals("RUNNING", result.status());
        assertEquals(1000, result.totalHashes());
        assertEquals(10, result.batchesExpected());
        assertEquals(5, result.batchesCompleted());
        assertEquals(25, result.foundCount());
        
        verify(jdbcTemplate, times(1)).query(
            contains("SELECT job_id, created_at, status, total_hashes, batches_expected, batches_completed, found_count"),
            any(org.springframework.jdbc.core.RowMapper.class),
            eq(jobId)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void getJobStatus_NonExistentJob_ReturnsNull() {
        // Arrange
        when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq(jobId)))
            .thenReturn(Collections.<JobRepository.JobStatus>emptyList());

        // Act
        JobRepository.JobStatus result = jobRepository.getJobStatus(jobId);

        // Assert
        assertNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    void listCompletedJobs_ReturnsCompletedJobs() {
        // Arrange
        JobRepository.JobStatus job1 = new JobRepository.JobStatus(
            UUID.randomUUID(),
            createdAt.minusSeconds(3600),
            "COMPLETED",
            1000,
            10,
            10,
            50
        );
        JobRepository.JobStatus job2 = new JobRepository.JobStatus(
            UUID.randomUUID(),
            createdAt.minusSeconds(1800),
            "COMPLETED",
            2000,
            20,
            20,
            100
        );

        when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn((List<JobRepository.JobStatus>) Arrays.asList(job1, job2));

        // Act
        List<JobRepository.JobStatus> result = jobRepository.listCompletedJobs();

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("COMPLETED", result.get(0).status());
        assertEquals("COMPLETED", result.get(1).status());
        
        verify(jdbcTemplate, times(1)).query(
            contains("SELECT job_id, created_at, status, total_hashes, batches_expected, batches_completed, found_count"),
            any(org.springframework.jdbc.core.RowMapper.class)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void listCompletedJobs_NoCompletedJobs_ReturnsEmptyList() {
        // Arrange
        when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(Collections.<JobRepository.JobStatus>emptyList());

        // Act
        List<JobRepository.JobStatus> result = jobRepository.listCompletedJobs();

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}

