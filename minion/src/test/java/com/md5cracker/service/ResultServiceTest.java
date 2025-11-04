package com.md5cracker.service;

import com.md5cracker.config.RabbitMQConfig;
import com.md5cracker.dto.HashBatch;
import com.md5cracker.dto.ResultBatch;
import com.md5cracker.dto.ResultItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ResultServiceTest {

    @Mock
    private HashLookupService lookupService;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @InjectMocks
    private ResultService resultService;

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
    void processBatch_WithFoundHashes_InsertsResultsAndPublishes() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1, hash2, hash3);
        HashBatch batch = new HashBatch(jobId, 0, hashes);
        
        Map<String, String> lookupResults = Map.of(
            hash1, "1234567890",
            hash2, "9876543210"
        );
        
        when(lookupService.lookupBatch(hashes)).thenReturn(lookupResults);
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1, 1});

        // Act
        resultService.processBatch(batch);

        // Assert
        verify(lookupService, times(1)).lookupBatch(hashes);
        
        // Verify database insert
        ArgumentCaptor<List<Object[]>> batchArgsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbcTemplate, times(1)).batchUpdate(
            eq("INSERT INTO results (job_id, hash_hex, phone_number, found_at) " +
               "VALUES (?, ?, ?, now()) " +
               "ON CONFLICT (job_id, hash_hex) DO NOTHING"),
            batchArgsCaptor.capture()
        );
        
        List<Object[]> batchArgs = batchArgsCaptor.getValue();
        assertEquals(2, batchArgs.size());
        assertEquals(jobId, batchArgs.get(0)[0]);
        assertEquals(hash1, batchArgs.get(0)[1]);
        assertEquals("1234567890", batchArgs.get(0)[2]);
        
        // Verify message published
        ArgumentCaptor<ResultBatch> resultBatchCaptor = ArgumentCaptor.forClass(ResultBatch.class);
        verify(rabbitTemplate, times(1)).convertAndSend(
            eq(RabbitMQConfig.EXCHANGE_NAME),
            eq(RabbitMQConfig.RESULTS_ROUTING_KEY),
            resultBatchCaptor.capture()
        );
        
        ResultBatch publishedBatch = resultBatchCaptor.getValue();
        assertEquals(jobId, publishedBatch.jobId());
        assertEquals(0, publishedBatch.batchIndex());
        assertEquals(2, publishedBatch.results().size());
        
        // Verify result items
        List<ResultItem> results = publishedBatch.results();
        assertTrue(results.stream().anyMatch(r -> r.hash().equals(hash1) && r.phone().equals("1234567890")));
        assertTrue(results.stream().anyMatch(r -> r.hash().equals(hash2) && r.phone().equals("9876543210")));
    }

    @Test
    void processBatch_WithNoFoundHashes_SkipsInsertButPublishesEmptyBatch() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1, hash2);
        HashBatch batch = new HashBatch(jobId, 1, hashes);
        
        when(lookupService.lookupBatch(hashes)).thenReturn(Collections.emptyMap());

        // Act
        resultService.processBatch(batch);

        // Assert
        verify(lookupService, times(1)).lookupBatch(hashes);
        verify(jdbcTemplate, never()).batchUpdate(anyString(), anyList());
        
        // Verify empty batch is still published
        ArgumentCaptor<ResultBatch> resultBatchCaptor = ArgumentCaptor.forClass(ResultBatch.class);
        verify(rabbitTemplate, times(1)).convertAndSend(
            eq(RabbitMQConfig.EXCHANGE_NAME),
            eq(RabbitMQConfig.RESULTS_ROUTING_KEY),
            resultBatchCaptor.capture()
        );
        
        ResultBatch publishedBatch = resultBatchCaptor.getValue();
        assertEquals(jobId, publishedBatch.jobId());
        assertEquals(1, publishedBatch.batchIndex());
        assertTrue(publishedBatch.results().isEmpty());
    }

    @Test
    void processBatch_WithAllHashesFound_InsertsAllResults() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1, hash2, hash3);
        HashBatch batch = new HashBatch(jobId, 2, hashes);
        
        Map<String, String> lookupResults = Map.of(
            hash1, "1111111111",
            hash2, "2222222222",
            hash3, "3333333333"
        );
        
        when(lookupService.lookupBatch(hashes)).thenReturn(lookupResults);
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1, 1, 1});

        // Act
        resultService.processBatch(batch);

        // Assert
        ArgumentCaptor<List<Object[]>> batchArgsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbcTemplate, times(1)).batchUpdate(anyString(), batchArgsCaptor.capture());
        
        List<Object[]> batchArgs = batchArgsCaptor.getValue();
        assertEquals(3, batchArgs.size());
        
        // Verify all results are in the published batch
        ArgumentCaptor<ResultBatch> resultBatchCaptor = ArgumentCaptor.forClass(ResultBatch.class);
        verify(rabbitTemplate, times(1)).convertAndSend(
            eq(RabbitMQConfig.EXCHANGE_NAME),
            eq(RabbitMQConfig.RESULTS_ROUTING_KEY),
            resultBatchCaptor.capture()
        );
        
        ResultBatch publishedBatch = resultBatchCaptor.getValue();
        assertEquals(3, publishedBatch.results().size());
    }

    @Test
    void processBatch_PreservesBatchIndex() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1);
        HashBatch batch = new HashBatch(jobId, 5, hashes);
        
        Map<String, String> lookupResults = Map.of(hash1, "1234567890");
        when(lookupService.lookupBatch(hashes)).thenReturn(lookupResults);
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1});

        // Act
        resultService.processBatch(batch);

        // Assert
        ArgumentCaptor<ResultBatch> resultBatchCaptor = ArgumentCaptor.forClass(ResultBatch.class);
        verify(rabbitTemplate, times(1)).convertAndSend(
            anyString(),
            anyString(),
            resultBatchCaptor.capture()
        );
        
        ResultBatch publishedBatch = resultBatchCaptor.getValue();
        assertEquals(5, publishedBatch.batchIndex());
    }
}

