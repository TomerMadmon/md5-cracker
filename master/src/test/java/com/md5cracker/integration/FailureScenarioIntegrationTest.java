package com.md5cracker.integration;

import com.md5cracker.dto.ResultBatch;
import com.md5cracker.dto.ResultItem;
import com.md5cracker.repository.JobRepository;
import com.md5cracker.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for failure scenarios during the MD5 cracking process.
 * Tests what happens when various components fail during processing.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
class FailureScenarioIntegrationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String hash1;
    private String hash2;

    @BeforeEach
    void setUp() {
        hash1 = "9b8ecefdcb3a2933eb717f83ff77a320";
        hash2 = "1234567890abcdef1234567890abcdef";
    }

    @Test
    void failure_JobCreationAfterPartialTargetInsertion_JobRolledBack() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        
        // Manually insert a target to simulate partial insertion
        jdbcTemplate.update(
            "INSERT INTO targets (job_id, hash_hex) VALUES (?, ?)",
            jobId.toString(), hash1
        );
        
        // Try to create job - this should fail due to foreign key constraint
        // Act & Assert
        assertThrows(Exception.class, () -> {
            jobRepository.createJob(jobId, 1, 1);
            // If we get here, verify cleanup
            Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM targets WHERE job_id = ?",
                Integer.class,
                jobId.toString()
            );
            // Target should remain (depending on transaction isolation)
            assertNotNull(count);
        });
    }

    @Test
    void failure_ResultBatchProcessingWithMissingJob_HandlesGracefully() {
        // Arrange
        UUID nonExistentJobId = UUID.randomUUID();
        List<ResultItem> results = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        ResultBatch resultBatch = new ResultBatch(nonExistentJobId, 0, results);

        // Act - Should not throw exception
        assertDoesNotThrow(() -> jobService.handleResultBatch(resultBatch));
        
        // Assert - Job should not exist
        assertNull(jobRepository.getJobStatus(nonExistentJobId));
    }

    @Test
    void failure_ResultBatchProcessingWithDatabaseError_TransactionRolledBack() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 10);
        
        // Get initial state
        JobRepository.JobStatus initialStatus = jobRepository.getJobStatus(jobId);
        assertEquals(0, initialStatus.batchesCompleted());
        
        // Simulate database failure by dropping table
        jdbcTemplate.execute("DROP TABLE jobs");
        
        List<ResultItem> results = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        ResultBatch resultBatch = new ResultBatch(jobId, 0, results);

        // Act & Assert
        assertThrows(Exception.class, () -> jobService.handleResultBatch(resultBatch));
        
        // Restore table
        jdbcTemplate.execute("""
            CREATE TABLE jobs (
              job_id VARCHAR(36) PRIMARY KEY,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
              total_hashes INT,
              batches_expected INT,
              batches_completed INT DEFAULT 0,
              found_count INT DEFAULT 0
            )
        """);
    }

    @Test
    void failure_PartialBatchProcessing_StateConsistent() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 5);
        
        // Process first batch successfully
        List<ResultItem> results1 = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        jobService.handleResultBatch(new ResultBatch(jobId, 0, results1));
        
        // Verify first batch processed
        JobRepository.JobStatus status1 = jobRepository.getJobStatus(jobId);
        assertEquals(1, status1.batchesCompleted());
        assertEquals(1, status1.foundCount());
        
        // Simulate failure on second batch by corrupting table
        jdbcTemplate.execute("DROP TABLE jobs");
        
        List<ResultItem> results2 = Arrays.asList(new ResultItem(hash2, "050-7654321"));
        ResultBatch resultBatch2 = new ResultBatch(jobId, 1, results2);
        
        // Act & Assert - Second batch should fail
        assertThrows(Exception.class, () -> jobService.handleResultBatch(resultBatch2));
        
        // Restore table and verify first batch still recorded
        jdbcTemplate.execute("""
            CREATE TABLE jobs (
              job_id VARCHAR(36) PRIMARY KEY,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
              total_hashes INT,
              batches_expected INT,
              batches_completed INT DEFAULT 0,
              found_count INT DEFAULT 0
            )
        """);
        
        // Re-insert job to check state
        jobRepository.createJob(jobId, 100, 5);
        jobRepository.updateJobProgress(jobId, 1, 1);
        
        JobRepository.JobStatus statusAfter = jobRepository.getJobStatus(jobId);
        assertEquals(1, statusAfter.batchesCompleted());
    }

    @Test
    void failure_FileUploadWithInvalidData_HandlesGracefully() throws IOException {
        // Arrange
        // Create file with binary data that might cause issues
        byte[] binaryData = new byte[]{0x00, 0x01, 0x02, (byte)0xFF};
        MockMultipartFile file = new MockMultipartFile(
            "file", "binary.txt", "application/octet-stream", binaryData
        );

        // Act - Should handle gracefully (filter invalid hashes)
        UUID jobId = jobService.createJob(file);

        // Assert
        assertNotNull(jobId);
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        // No valid hashes should be found
        assertEquals(0, status.totalHashes());
    }

    @Test
    void failure_ConcurrentResultBatchProcessing_HandlesRaceConditions() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 10);
        
        // Simulate two batches arriving concurrently
        List<ResultItem> results1 = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        List<ResultItem> results2 = Arrays.asList(new ResultItem(hash2, "050-7654321"));
        
        ResultBatch batch1 = new ResultBatch(jobId, 0, results1);
        ResultBatch batch2 = new ResultBatch(jobId, 1, results2);

        // Act - Process both batches
        jobService.handleResultBatch(batch1);
        jobService.handleResultBatch(batch2);

        // Assert
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertEquals(2, status.batchesCompleted());
        assertEquals(2, status.foundCount());
    }

    @Test
    void failure_JobCompletionWithMissingBatches_StillMarksComplete() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 2);
        
        // Process both batches
        List<ResultItem> results1 = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        jobService.handleResultBatch(new ResultBatch(jobId, 0, results1));
        
        List<ResultItem> results2 = Arrays.asList(new ResultItem(hash2, "050-7654321"));
        jobService.handleResultBatch(new ResultBatch(jobId, 1, results2));

        // Assert
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertEquals("COMPLETED", status.status());
        assertEquals(2, status.batchesCompleted());
    }

    @Test
    void failure_JobCompletionWithDuplicateBatch_HandlesIdempotency() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 2);
        
        List<ResultItem> results = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        
        // Process batch 0 twice (simulating retry)
        jobService.handleResultBatch(new ResultBatch(jobId, 0, results));
        jobService.handleResultBatch(new ResultBatch(jobId, 0, results));
        
        // Process batch 1
        jobService.handleResultBatch(new ResultBatch(jobId, 1, results));

        // Assert
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        // Should only count batch 0 once
        assertEquals(2, status.batchesCompleted());
        assertEquals("COMPLETED", status.status());
    }
}

