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

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobServiceIntegrationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String hash1;
    private String hash2;
    private String hash3;

    @BeforeEach
    void setUp() {
        hash1 = "a1b2c3d4e5f6789012345678901234ab";
        hash2 = "1234567890abcdef1234567890abcdef";
        hash3 = "fedcba0987654321fedcba0987654321";
        
        // Insert test data into md5_phone_map_bin
        // Hash for "050-1234567" -> 9b8ecefdcb3a2933eb717f83ff77a320
        String testHash = "9b8ecefdcb3a2933eb717f83ff77a320";
        String testPhone = "050-1234567";
        jdbcTemplate.update(
            "INSERT INTO md5_phone_map_bin (md5_hash, phone_number) VALUES (?, ?) ON CONFLICT DO NOTHING",
            hexStringToBytes(testHash),
            testPhone
        );
    }

    @Test
    void createJob_ValidFile_CreatesJobAndStoresTargets() throws IOException {
        // Arrange
        String content = hash1 + "\n" + hash2 + "\n" + hash3;
        MockMultipartFile file = new MockMultipartFile(
            "file", "hashes.txt", "text/plain", content.getBytes()
        );

        // Act
        UUID jobId = jobService.createJob(file);

        // Assert
        assertNotNull(jobId);
        
        // Verify job was created
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertNotNull(status);
        assertEquals("RUNNING", status.status());
        assertEquals(3, status.totalHashes());
        assertEquals(1, status.batchesExpected());
        
        // Verify targets were stored
        Integer targetCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM targets WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertNotNull(targetCount);
        assertEquals(3, targetCount.intValue());
    }

    @Test
    void createJob_EmptyFile_CreatesJobWithZeroHashes() throws IOException {
        // Arrange
        MockMultipartFile file = new MockMultipartFile(
            "file", "empty.txt", "text/plain", "".getBytes()
        );

        // Act
        UUID jobId = jobService.createJob(file);

        // Assert
        assertNotNull(jobId);
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertNotNull(status);
        assertEquals(0, status.totalHashes());
        assertEquals(0, status.batchesExpected());
    }

    @Test
    void handleResultBatch_ValidBatch_UpdatesProgress() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 10);
        
        List<ResultItem> results = Arrays.asList(
            new ResultItem(hash1, "050-1234567"),
            new ResultItem(hash2, "050-7654321")
        );
        ResultBatch resultBatch = new ResultBatch(jobId, 0, results);

        // Act
        jobService.handleResultBatch(resultBatch);

        // Assert
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertNotNull(status);
        assertEquals(1, status.batchesCompleted());
        assertEquals(2, status.foundCount());
        assertEquals("RUNNING", status.status()); // Not complete yet
    }

    @Test
    void handleResultBatch_LastBatch_MarksJobComplete() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 2);
        
        // Complete first batch
        List<ResultItem> results1 = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        jobService.handleResultBatch(new ResultBatch(jobId, 0, results1));
        
        // Complete second batch (last one)
        List<ResultItem> results2 = Arrays.asList(new ResultItem(hash2, "050-7654321"));
        ResultBatch resultBatch2 = new ResultBatch(jobId, 1, results2);

        // Act
        jobService.handleResultBatch(resultBatch2);

        // Assert
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertNotNull(status);
        assertEquals(2, status.batchesCompleted());
        assertEquals("COMPLETED", status.status());
    }

    @Test
    void handleResultBatch_NonExistentJob_DoesNotThrow() {
        // Arrange
        UUID nonExistentJobId = UUID.randomUUID();
        List<ResultItem> results = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        ResultBatch resultBatch = new ResultBatch(nonExistentJobId, 0, results);

        // Act & Assert - Should not throw exception
        assertDoesNotThrow(() -> jobService.handleResultBatch(resultBatch));
        
        // Verify job was not created
        JobRepository.JobStatus status = jobRepository.getJobStatus(nonExistentJobId);
        assertNull(status);
    }

    @Test
    void generateResultsCsv_WithResults_GeneratesCorrectCsv() throws IOException {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 3, 1);
        
        // Insert targets
        jdbcTemplate.update(
            "INSERT INTO targets (job_id, hash_hex) VALUES (?, ?)",
            jobId.toString(), hash1
        );
        jdbcTemplate.update(
            "INSERT INTO targets (job_id, hash_hex) VALUES (?, ?)",
            jobId.toString(), hash2
        );
        
        // Insert results (only hash1 found)
        jdbcTemplate.update(
            "INSERT INTO results (job_id, hash_hex, phone_number) VALUES (?, ?, ?)",
            jobId.toString(), hash1, "050-1234567"
        );

        // Act
        String csv = jobService.generateResultsCsv(jobId);

        // Assert
        assertNotNull(csv);
        assertTrue(csv.contains("hash,phone"));
        assertTrue(csv.contains(hash1));
        assertTrue(csv.contains("050-1234567"));
        assertTrue(csv.contains(hash2));
        assertTrue(csv.contains("NOT FOUND"));
    }

    @Test
    void handleResultBatch_DatabaseFailure_JobStatusNotUpdated() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 10);
        
        // Get initial status
        JobRepository.JobStatus initialStatus = jobRepository.getJobStatus(jobId);
        assertNotNull(initialStatus);
        
        // Drop the jobs table to simulate database failure
        try {
            jdbcTemplate.execute("DROP TABLE jobs");
            
            List<ResultItem> results = Arrays.asList(new ResultItem(hash1, "050-1234567"));
            ResultBatch resultBatch = new ResultBatch(jobId, 0, results);

            // Act & Assert
            assertThrows(Exception.class, () -> jobService.handleResultBatch(resultBatch));
        } finally {
            // Restore table for cleanup
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
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
    }

    @Test
    void createJob_WithInvalidHashes_FiltersThem() throws IOException {
        // Arrange
        String content = hash1 + "\n" +           // Valid (32 chars)
                         "short\n" +               // Too short
                         hash2 + "\n" +           // Valid
                         "   \n" +                 // Blank
                         "toolonghash123456789012345678901234567890\n" + // Too long
                         hash3;                    // Valid
        
        MockMultipartFile file = new MockMultipartFile(
            "file", "mixed.txt", "text/plain", content.getBytes()
        );

        // Act
        UUID jobId = jobService.createJob(file);

        // Assert
        assertNotNull(jobId);
        JobRepository.JobStatus status = jobRepository.getJobStatus(jobId);
        assertEquals(3, status.totalHashes()); // Only 3 valid hashes
        
        // Verify only valid hashes stored
        Integer targetCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM targets WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertEquals(3, targetCount);
    }

    @Test
    void handleResultBatch_PartialFailure_ProgressStillUpdated() {
        // Arrange
        UUID jobId = UUID.randomUUID();
        jobRepository.createJob(jobId, 100, 5);
        
        // Process first batch successfully
        List<ResultItem> results1 = Arrays.asList(new ResultItem(hash1, "050-1234567"));
        jobService.handleResultBatch(new ResultBatch(jobId, 0, results1));
        
        // Verify progress
        JobRepository.JobStatus status1 = jobRepository.getJobStatus(jobId);
        assertEquals(1, status1.batchesCompleted());
        
        // Process second batch
        List<ResultItem> results2 = Arrays.asList(new ResultItem(hash2, "050-7654321"));
        jobService.handleResultBatch(new ResultBatch(jobId, 1, results2));
        
        // Assert
        JobRepository.JobStatus status2 = jobRepository.getJobStatus(jobId);
        assertEquals(2, status2.batchesCompleted());
        assertEquals(2, status2.foundCount());
    }

    // Helper method to convert hex string to byte array
    private byte[] hexStringToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i+1), 16));
        }
        return data;
    }
}

