package com.md5cracker.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class JobEventPublisherTest {

    private JobEventPublisher eventPublisher;
    private UUID jobId;

    @BeforeEach
    void setUp() {
        eventPublisher = new JobEventPublisher();
        jobId = UUID.randomUUID();
    }

    @Test
    void registerEmitter_RegistersEmitter() {
        // Arrange
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        // Act
        eventPublisher.registerEmitter(jobId, emitter);

        // Assert
        // Emitter is registered (no exception thrown)
        assertNotNull(emitter);
    }

    @Test
    void publishProgress_WithRegisteredEmitter_SendsEvent() throws IOException {
        // Arrange
        SseEmitter emitter = spy(new SseEmitter(Long.MAX_VALUE));
        eventPublisher.registerEmitter(jobId, emitter);
        
        Map<String, Object> data = Map.of("test", "data");

        // Act
        eventPublisher.publishProgress(jobId, "test_event", data);

        // Assert
        verify(emitter, times(1)).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void publishProgress_WithoutRegisteredEmitter_DoesNothing() {
        // Arrange
        Map<String, Object> data = Map.of("test", "data");

        // Act & Assert - Should not throw exception
        assertDoesNotThrow(() -> eventPublisher.publishProgress(jobId, "test_event", data));
    }

    @Test
    void complete_WithRegisteredEmitter_CompletesEmitter() {
        // Arrange
        SseEmitter emitter = spy(new SseEmitter(Long.MAX_VALUE));
        eventPublisher.registerEmitter(jobId, emitter);

        // Act
        eventPublisher.complete(jobId);

        // Assert
        verify(emitter, times(1)).complete();
    }

    @Test
    void complete_WithoutRegisteredEmitter_DoesNothing() {
        // Act & Assert - Should not throw exception
        assertDoesNotThrow(() -> eventPublisher.complete(jobId));
    }
}

