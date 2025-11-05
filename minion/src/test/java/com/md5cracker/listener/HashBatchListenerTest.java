package com.md5cracker.listener;

import com.md5cracker.dto.HashBatch;
import com.md5cracker.service.ResultService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HashBatchListenerTest {

    @Mock
    private ResultService resultService;

    @InjectMocks
    private HashBatchListener hashBatchListener;

    private UUID jobId;
    private List<String> hashes;

    @BeforeEach
    void setUp() {
        jobId = UUID.randomUUID();
        hashes = Arrays.asList(
            "a1b2c3d4e5f6789012345678901234ab",
            "1234567890abcdef1234567890abcdef"
        );
    }

    @Test
    void handleHashBatch_ValidBatch_ProcessesBatch() {
        // Arrange
        HashBatch batch = new HashBatch(jobId, 0, hashes);
        doNothing().when(resultService).processBatch(any(HashBatch.class));

        // Act
        hashBatchListener.handleHashBatch(batch);

        // Assert
        verify(resultService, times(1)).processBatch(batch);
    }

    @Test
    void handleHashBatch_EmptyBatch_StillProcesses() {
        // Arrange
        HashBatch batch = new HashBatch(jobId, 1, List.of());
        doNothing().when(resultService).processBatch(any(HashBatch.class));

        // Act
        hashBatchListener.handleHashBatch(batch);

        // Assert
        verify(resultService, times(1)).processBatch(batch);
    }

    @Test
    void handleHashBatch_MultipleBatches_ProcessesEach() {
        // Arrange
        HashBatch batch1 = new HashBatch(jobId, 0, hashes);
        HashBatch batch2 = new HashBatch(jobId, 1, hashes);
        
        doNothing().when(resultService).processBatch(any(HashBatch.class));

        // Act
        hashBatchListener.handleHashBatch(batch1);
        hashBatchListener.handleHashBatch(batch2);

        // Assert
        verify(resultService, times(2)).processBatch(any(HashBatch.class));
        verify(resultService, times(1)).processBatch(batch1);
        verify(resultService, times(1)).processBatch(batch2);
    }

    @Test
    void handleHashBatch_ExceptionInService_PropagatesException() {
        // Arrange
        HashBatch batch = new HashBatch(jobId, 0, hashes);
        doThrow(new RuntimeException("Service error")).when(resultService).processBatch(any(HashBatch.class));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> hashBatchListener.handleHashBatch(batch));
        verify(resultService, times(1)).processBatch(batch);
    }
}

