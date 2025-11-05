package com.md5cracker.integration;

import com.md5cracker.dto.HashBatch;
import com.md5cracker.service.ResultService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class ResultServiceIntegrationTest {

    @Autowired
    private ResultService resultService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UUID jobId;
    private String hash1;
    private String hash2;
    private String hash3;
    private String phone1;
    private String phone2;

    @BeforeEach
    void setUp() {
        jobId = UUID.randomUUID();
        hash1 = "9b8ecefdcb3a2933eb717f83ff77a320"; // MD5 of "050-1234567"
        hash2 = "1234567890abcdef1234567890abcdef"; // Test hash
        hash3 = "fedcba0987654321fedcba0987654321"; // Test hash
        phone1 = "050-1234567";
        phone2 = "050-7654321";
        
        // Insert test data into md5_phone_map_bin
        jdbcTemplate.update(
            "INSERT INTO md5_phone_map_bin (md5_hash, phone_number) VALUES (?, ?) ON CONFLICT DO NOTHING",
            hexStringToBytes(hash1),
            phone1
        );
        jdbcTemplate.update(
            "INSERT INTO md5_phone_map_bin (md5_hash, phone_number) VALUES (?, ?) ON CONFLICT DO NOTHING",
            hexStringToBytes(hash2),
            phone2
        );
    }

    @Test
    void processBatch_WithFoundHashes_InsertsResults() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1, hash2, hash3);
        HashBatch batch = new HashBatch(jobId, 0, hashes);

        // Act
        resultService.processBatch(batch);

        // Assert
        // Verify results were inserted (hash1 and hash2 should be found)
        Integer resultCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM results WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertEquals(2, resultCount); // hash1 and hash2 found, hash3 not found
        
        // Verify specific results
        String phone1Result = jdbcTemplate.queryForObject(
            "SELECT phone_number FROM results WHERE job_id = ? AND hash_hex = ?",
            String.class,
            jobId.toString(),
            hash1
        );
        assertEquals(phone1, phone1Result);
        
        String phone2Result = jdbcTemplate.queryForObject(
            "SELECT phone_number FROM results WHERE job_id = ? AND hash_hex = ?",
            String.class,
            jobId.toString(),
            hash2
        );
        assertEquals(phone2, phone2Result);
    }

    @Test
    void processBatch_WithNoFoundHashes_InsertsNothing() {
        // Arrange
        List<String> hashes = Arrays.asList(hash3); // hash3 not in database
        HashBatch batch = new HashBatch(jobId, 1, hashes);

        // Act
        resultService.processBatch(batch);

        // Assert
        Integer resultCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM results WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertEquals(0, resultCount);
    }

    @Test
    void processBatch_WithDuplicateHashes_HandlesIdempotency() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1, hash1, hash1); // Same hash multiple times
        HashBatch batch = new HashBatch(jobId, 0, hashes);

        // Act
        resultService.processBatch(batch);
        
        // Process same batch again (simulating retry)
        resultService.processBatch(batch);

        // Assert
        // Should only have one result due to ON CONFLICT DO NOTHING
        Integer resultCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM results WHERE job_id = ? AND hash_hex = ?",
            Integer.class,
            jobId.toString(),
            hash1
        );
        assertEquals(1, resultCount);
    }

    @Test
    void processBatch_DatabaseFailure_ThrowsException() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1);
        HashBatch batch = new HashBatch(jobId, 0, hashes);
        
        // Drop the results table to simulate database failure
        jdbcTemplate.execute("DROP TABLE results");

        // Act & Assert
        assertThrows(Exception.class, () -> resultService.processBatch(batch));
        
        // Restore table for cleanup
        jdbcTemplate.execute("""
            CREATE TABLE results (
              job_id VARCHAR(36),
              hash_hex CHAR(32) NOT NULL,
              phone_number CHAR(11),
              found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (job_id, hash_hex)
            )
        """);
    }

    @Test
    void processBatch_WithLargeBatch_ProcessesAll() {
        // Arrange - Create 1000 hashes (only hash1 and hash2 are in DB)
        String[] hashesArray = new String[1000];
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0) {
                hashesArray[i] = hash1;
            } else {
                hashesArray[i] = hash2;
            }
        }
        List<String> hashes = Arrays.asList(hashesArray);
        HashBatch batch = new HashBatch(jobId, 0, hashes);

        // Act
        resultService.processBatch(batch);

        // Assert
        // Should have 2 results (hash1 and hash2) even though processed 1000 times
        Integer resultCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM results WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertEquals(2, resultCount);
    }

    @Test
    void processBatch_WithEmptyBatch_DoesNotFail() {
        // Arrange
        List<String> hashes = Arrays.asList();
        HashBatch batch = new HashBatch(jobId, 0, hashes);

        // Act & Assert
        assertDoesNotThrow(() -> resultService.processBatch(batch));
        
        Integer resultCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM results WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertEquals(0, resultCount);
    }

    @Test
    void processBatch_WithInvalidHashFormat_HandlesGracefully() {
        // Arrange - Include invalid hash formats
        List<String> hashes = Arrays.asList(
            hash1,                    // Valid
            "short",                  // Too short
            "1234567890abcdef1234567890abcdef1234567890", // Too long
            "",                       // Empty
            hash2                     // Valid
        );
        HashBatch batch = new HashBatch(jobId, 0, hashes);

        // Act
        resultService.processBatch(batch);

        // Assert
        // Only valid hashes should be processed
        Integer resultCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM results WHERE job_id = ?",
            Integer.class,
            jobId.toString()
        );
        assertEquals(2, resultCount); // hash1 and hash2
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

