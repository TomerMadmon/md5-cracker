package com.md5cracker.service;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class JobEventPublisher {
    
    private final Map<UUID, SseEmitter> emitters = new ConcurrentHashMap<>();

    public void registerEmitter(UUID jobId, SseEmitter emitter) {
        emitter.onCompletion(() -> emitters.remove(jobId));
        emitter.onTimeout(() -> emitters.remove(jobId));
        emitter.onError(ex -> emitters.remove(jobId));
        emitters.put(jobId, emitter);
    }

    public void publishProgress(UUID jobId, String type, Object data) {
        SseEmitter emitter = emitters.get(jobId);
        if (emitter != null) {
            try {
                Map<String, Object> event = Map.of(
                    "type", type,
                    "payload", data
                );
                emitter.send(SseEmitter.event()
                    .name("message")
                    .data(event));
            } catch (IOException e) {
                emitters.remove(jobId);
                emitter.completeWithError(e);
            }
        }
    }

    public void complete(UUID jobId) {
        SseEmitter emitter = emitters.get(jobId);
        if (emitter != null) {
            emitter.complete();
            emitters.remove(jobId);
        }
    }
}

