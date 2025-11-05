package com.md5cracker.controller;

import com.md5cracker.repository.JobRepository;
import com.md5cracker.service.JobEventPublisher;
import com.md5cracker.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobControllerTest {

    @Mock
    private JobService jobService;

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobEventPublisher eventPublisher;

    @InjectMocks
    private JobController jobController;

    private UUID jobId;
    private JobRepository.JobStatus jobStatus;

    @BeforeEach
    void setUp() {
        jobId = UUID.randomUUID();
        jobStatus = new JobRepository.JobStatus(
            jobId,
            Instant.now(),
            "RUNNING",
            100,
            10,
            5,
            25
        );
    }

    @Test
    void upload_ValidFile_ReturnsAcceptedWithJobId() throws IOException {
        // Arrange
        String content = "a1b2c3d4e5f6789012345678901234ab\n1234567890abcdef1234567890abcdef";
        MultipartFile file = new MockMultipartFile(
            "file",
            "hashes.txt",
            "text/plain",
            content.getBytes()
        );

        when(jobService.createJob(file)).thenReturn(jobId);

        // Act
        ResponseEntity<?> response = jobController.upload(file);

        // Assert
        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertNotNull(response.getBody());
        verify(jobService, times(1)).createJob(file);
        verify(eventPublisher, times(1)).publishProgress(eq(jobId), eq("job_created"), any());
    }

    @Test
    void upload_IOException_ThrowsException() throws IOException {
        // Arrange
        MultipartFile file = new MockMultipartFile(
            "file",
            "hashes.txt",
            "text/plain",
            "content".getBytes()
        );

        when(jobService.createJob(file)).thenThrow(new IOException("File read error"));

        // Act & Assert
        assertThrows(IOException.class, () -> jobController.upload(file));
    }

    @Test
    void listJobs_ReturnsCompletedJobs() {
        // Arrange
        List<JobRepository.JobStatus> completedJobs = Arrays.asList(
            new JobRepository.JobStatus(
                UUID.randomUUID(),
                Instant.now().minusSeconds(3600),
                "COMPLETED",
                100,
                10,
                10,
                50
            ),
            new JobRepository.JobStatus(
                UUID.randomUUID(),
                Instant.now().minusSeconds(1800),
                "COMPLETED",
                200,
                20,
                20,
                100
            )
        );

        when(jobRepository.listCompletedJobs()).thenReturn(completedJobs);

        // Act
        ResponseEntity<List<JobRepository.JobStatus>> response = jobController.listJobs();

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(2, response.getBody().size());
        assertEquals("COMPLETED", response.getBody().get(0).status());
        verify(jobRepository, times(1)).listCompletedJobs();
    }

    @Test
    void listJobs_NoCompletedJobs_ReturnsEmptyList() {
        // Arrange
        when(jobRepository.listCompletedJobs()).thenReturn(Collections.emptyList());

        // Act
        ResponseEntity<List<JobRepository.JobStatus>> response = jobController.listJobs();

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty());
    }

    @Test
    void getJob_ExistingJob_ReturnsJobStatus() {
        // Arrange
        when(jobRepository.getJobStatus(jobId)).thenReturn(jobStatus);

        // Act
        ResponseEntity<?> response = jobController.getJob(jobId);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(jobStatus, response.getBody());
        verify(jobRepository, times(1)).getJobStatus(jobId);
    }

    @Test
    void getJob_NonExistentJob_ReturnsNotFound() {
        // Arrange
        when(jobRepository.getJobStatus(jobId)).thenReturn(null);

        // Act
        ResponseEntity<?> response = jobController.getJob(jobId);

        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(jobRepository, times(1)).getJobStatus(jobId);
    }

    @Test
    void getEvents_RegistersEmitter() {
        // Act
        SseEmitter emitter = jobController.getEvents(jobId);

        // Assert
        assertNotNull(emitter);
        verify(eventPublisher, times(1)).registerEmitter(jobId, emitter);
    }

    @Test
    void getResults_ExistingJob_ReturnsCsv() {
        // Arrange
        String csvContent = "hash,phone\nabc123,NOT FOUND\ndef456,1234567890";
        
        when(jobRepository.getJobStatus(jobId)).thenReturn(jobStatus);
        when(jobService.generateResultsCsv(jobId)).thenReturn(csvContent);

        // Act
        ResponseEntity<?> response = jobController.getResults(jobId);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(csvContent, response.getBody());
        assertEquals("attachment; filename=\"" + jobId + "-results.csv\"", 
                     response.getHeaders().getFirst("Content-Disposition"));
        assertEquals(org.springframework.http.MediaType.TEXT_PLAIN, 
                     response.getHeaders().getContentType());
        verify(jobRepository, times(1)).getJobStatus(jobId);
        verify(jobService, times(1)).generateResultsCsv(jobId);
    }

    @Test
    void getResults_NonExistentJob_ReturnsNotFound() {
        // Arrange
        when(jobRepository.getJobStatus(jobId)).thenReturn(null);

        // Act
        ResponseEntity<?> response = jobController.getResults(jobId);

        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(jobRepository, times(1)).getJobStatus(jobId);
        verify(jobService, never()).generateResultsCsv(any());
    }
}

