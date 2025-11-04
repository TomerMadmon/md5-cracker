package com.md5cracker.listener;

import com.md5cracker.dto.HashBatch;
import com.md5cracker.service.ResultService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class HashBatchListener {
    
    private final ResultService resultService;

    public HashBatchListener(ResultService resultService) {
        this.resultService = resultService;
    }

    @RabbitListener(queues = "md5.lookup")
    public void handleHashBatch(HashBatch batch) {
        try {
            resultService.processBatch(batch);
        } catch (Exception e) {
            // Log error and rethrow to trigger retry
            throw new RuntimeException("Failed to process batch " + batch.batchIndex() + " for job " + batch.jobId(), e);
        }
    }
}

