package com.md5cracker.service;

import com.md5cracker.config.RabbitMQConfig;
import com.md5cracker.dto.HashBatch;
import com.md5cracker.dto.ResultBatch;
import com.md5cracker.dto.ResultItem;
import com.md5cracker.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private JobEventPublisher eventPublisher;

    @InjectMocks
    private JobService jobService;

    private UUID jobId;
    private String hash1;
    private String hash2;
    private String hash3;

    @BeforeEach
    void setUp() {
        jobId = UUID.randomUUID();
        hash1 = "a1b2c3d4e5f6789012345678901234ab";
        hash2 = "1234567890abcdef1234567890abcdef";
        hash3 = "fedcba0987654321fedcba0987654321";
    }

    @Test
    void createJob_ValidFile_CreatesJobAndPublishesBatches() throws IOException {
        // Arrange
        String content = hash1 + "\n" + hash2 + "\n" + hash3;
        MultipartFile file = new MockMultipartFile(
            "file",
            "hashes.txt",
            "text/plain",
            content.getBytes()
        );

        doNothing().when(jobRepository).createJob(any(), anyInt(), anyInt());
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1, 1, 1});

        // Act
        UUID result = jobService.createJob(file);

        // Assert
        assertNotNull(result);
        
        // Verify job creation
        ArgumentCaptor<UUID> jobIdCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<Integer> totalHashesCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> batchesCaptor = ArgumentCaptor.forClass(Integer.class);
        
        verify(jobRepository, times(1)).createJob(
            jobIdCaptor.capture(),
            totalHashesCaptor.capture(),
            batchesCaptor.capture()
        );
        assertEquals(3, totalHashesCaptor.getValue());
        assertEquals(1, batchesCaptor.getValue()); // 3 hashes < 1000 batch size
        
        // Verify targets stored
        verify(jdbcTemplate, times(1)).batchUpdate(anyString(), anyList());
        
        // Verify batches published
        ArgumentCaptor<HashBatch> batchCaptor = ArgumentCaptor.forClass(HashBatch.class);
        verify(rabbitTemplate, times(1)).convertAndSend(
            eq(RabbitMQConfig.EXCHANGE_NAME),
            eq(RabbitMQConfig.LOOKUP_ROUTING_KEY),
            batchCaptor.capture()
        );
        
        HashBatch publishedBatch = batchCaptor.getValue();
        assertEquals(jobIdCaptor.getValue(), publishedBatch.jobId());
        assertEquals(0, publishedBatch.batchIndex());
        assertEquals(3, publishedBatch.hashes().size());
    }

    @Test
    void createJob_LargeFile_CreatesMultipleBatches() throws IOException {
        // Arrange - Create 2500 hashes (should create 3 batches of 1000, 1000, 500)
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 2500; i++) {
            content.append(String.format("%032x", i)).append("\n");
        }
        
        MultipartFile file = new MockMultipartFile(
            "file",
            "large_hashes.txt",
            "text/plain",
            content.toString().getBytes()
        );

        doNothing().when(jobRepository).createJob(any(), anyInt(), anyInt());
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[2500]);

        // Act
        jobService.createJob(file);

        // Assert
        ArgumentCaptor<UUID> jobIdCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<Integer> totalHashesCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> batchesCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(jobRepository, times(1)).createJob(
            jobIdCaptor.capture(),
            totalHashesCaptor.capture(),
            batchesCaptor.capture()
        );
        assertEquals(2500, totalHashesCaptor.getValue());
        assertEquals(3, batchesCaptor.getValue());
        
        // Verify 3 batches published
        ArgumentCaptor<HashBatch> batchCaptor = ArgumentCaptor.forClass(HashBatch.class);
        verify(rabbitTemplate, times(3)).convertAndSend(
            eq(RabbitMQConfig.EXCHANGE_NAME),
            eq(RabbitMQConfig.LOOKUP_ROUTING_KEY),
            batchCaptor.capture()
        );
        
        List<HashBatch> batches = batchCaptor.getAllValues();
        assertEquals(0, batches.get(0).batchIndex());
        assertEquals(1, batches.get(1).batchIndex());
        assertEquals(2, batches.get(2).batchIndex());
        assertEquals(1000, batches.get(0).hashes().size());
        assertEquals(1000, batches.get(1).hashes().size());
        assertEquals(500, batches.get(2).hashes().size());
    }

    @Test
    void createJob_FiltersInvalidHashes() throws IOException {
        // Arrange - Mix valid and invalid hashes
        String content = hash1 + "\n" +           // Valid (32 chars)
                         "short\n" +               // Too short
                         hash2 + "\n" +           // Valid
                         "   \n" +                 // Blank
                         "toolonghash123456789012345678901234567890\n" + // Too long
                         hash3;                    // Valid
        
        MultipartFile file = new MockMultipartFile(
            "file",
            "mixed_hashes.txt",
            "text/plain",
            content.getBytes()
        );

        doNothing().when(jobRepository).createJob(any(), anyInt(), anyInt());
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1, 1, 1});

        // Act
        UUID result = jobService.createJob(file);

        // Assert
        assertNotNull(result);
        
        ArgumentCaptor<Integer> totalHashesCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(jobRepository, times(1)).createJob(any(), totalHashesCaptor.capture(), anyInt());
        assertEquals(3, totalHashesCaptor.getValue()); // Only 3 valid hashes
    }

    @Test
    void createJob_EmptyFile_ThrowsException() throws IOException {
        // Arrange
        MultipartFile file = new MockMultipartFile(
            "file",
            "empty.txt",
            "text/plain",
            "".getBytes()
        );

        // Act
        UUID result = jobService.createJob(file);

        // Assert
        assertNotNull(result);
        verify(jobRepository, times(1)).createJob(any(), eq(0), eq(0));
        verify(rabbitTemplate, never()).convertAndSend(anyString(), anyString(), any(Object.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void generateResultsCsv_WithResults_GeneratesCorrectCsv() throws Exception {
        // Arrange - Create ResultRow objects using reflection (since it's a private record)
        java.lang.reflect.Constructor<?> constructor = 
            Class.forName("com.md5cracker.service.JobService$ResultRow")
                .getDeclaredConstructor(String.class, String.class);
        constructor.setAccessible(true);
        
        Object row1 = constructor.newInstance(hash1, "1234567890");
        Object row2 = constructor.newInstance(hash2, "NOT FOUND");
        Object row3 = constructor.newInstance(hash3, "9876543210");
        
        when(jdbcTemplate.query(
            anyString(), 
            any(org.springframework.jdbc.core.RowMapper.class), 
            eq(jobId)
        )).thenReturn((List<Object>) Arrays.asList(row1, row2, row3));

        // Act
        String csv = jobService.generateResultsCsv(jobId);

        // Assert
        assertNotNull(csv);
        assertTrue(csv.contains("hash,phone"));
        assertTrue(csv.contains(hash1 + ",1234567890"));
        assertTrue(csv.contains(hash2 + ",NOT FOUND"));
        assertTrue(csv.contains(hash3 + ",9876543210"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void generateResultsCsv_NoResults_ReturnsHeaderOnly() {
        // Arrange
        when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq(jobId)))
            .thenReturn((List<Object>) Collections.emptyList());

        // Act
        String csv = jobService.generateResultsCsv(jobId);

        // Assert
        assertEquals("hash,phone\n", csv);
    }

    @Test
    void handleResultBatch_ValidBatch_UpdatesProgress() {
        // Arrange
        List<ResultItem> results = Arrays.asList(
            new ResultItem(hash1, "1234567890"),
            new ResultItem(hash2, "9876543210")
        );
        ResultBatch resultBatch = new ResultBatch(jobId, 0, results);
        
        JobRepository.JobStatus status = new JobRepository.JobStatus(
            jobId, Instant.now(), "RUNNING", 2000, 2, 0, 0
        );

        when(jobRepository.getJobStatus(jobId)).thenReturn(status);
        doNothing().when(jobRepository).updateJobProgress(any(), anyInt(), anyInt());
        doNothing().when(eventPublisher).publishProgress(any(), anyString(), any());

        // Act
        jobService.handleResultBatch(resultBatch);

        // Assert
        verify(jobRepository, times(1)).getJobStatus(jobId);
        verify(jobRepository, times(1)).updateJobProgress(jobId, 1, 2);
        verify(eventPublisher, times(1)).publishProgress(eq(jobId), eq("progress"), any());
        verify(jobRepository, never()).markJobComplete(any());
    }

    @Test
    void handleResultBatch_LastBatch_MarksJobComplete() {
        // Arrange
        List<ResultItem> results = Arrays.asList(
            new ResultItem(hash1, "1234567890")
        );
        ResultBatch resultBatch = new ResultBatch(jobId, 1, results);
        
        JobRepository.JobStatus status = new JobRepository.JobStatus(
            jobId, Instant.now(), "RUNNING", 2000, 2, 1, 10
        );

        when(jobRepository.getJobStatus(jobId)).thenReturn(status);
        doNothing().when(jobRepository).updateJobProgress(any(), anyInt(), anyInt());
        doNothing().when(jobRepository).markJobComplete(any());
        doNothing().when(eventPublisher).publishProgress(any(), anyString(), any());
        doNothing().when(eventPublisher).complete(any());

        // Act
        jobService.handleResultBatch(resultBatch);

        // Assert
        verify(jobRepository, times(1)).updateJobProgress(jobId, 2, 11);
        verify(jobRepository, times(1)).markJobComplete(jobId);
        verify(eventPublisher, times(1)).publishProgress(eq(jobId), eq("completed"), any());
        verify(eventPublisher, times(1)).complete(jobId);
    }

    @Test
    void handleResultBatch_NonExistentJob_IgnoresBatch() {
        // Arrange
        List<ResultItem> results = Arrays.asList(
            new ResultItem(hash1, "1234567890")
        );
        ResultBatch resultBatch = new ResultBatch(jobId, 0, results);

        when(jobRepository.getJobStatus(jobId)).thenReturn(null);

        // Act
        jobService.handleResultBatch(resultBatch);

        // Assert
        verify(jobRepository, times(1)).getJobStatus(jobId);
        verify(jobRepository, never()).updateJobProgress(any(), anyInt(), anyInt());
        verify(eventPublisher, never()).publishProgress(any(), anyString(), any());
    }
}

