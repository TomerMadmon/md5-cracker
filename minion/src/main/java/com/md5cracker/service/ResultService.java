package com.md5cracker.service;

import com.md5cracker.config.RabbitMQConfig;
import com.md5cracker.dto.HashBatch;
import com.md5cracker.dto.ResultBatch;
import com.md5cracker.dto.ResultItem;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class ResultService {
    
    private final HashLookupService lookupService;
    private final JdbcTemplate jdbcTemplate;
    private final RabbitTemplate rabbitTemplate;

    public ResultService(HashLookupService lookupService, JdbcTemplate jdbcTemplate, 
                        RabbitTemplate rabbitTemplate) {
        this.lookupService = lookupService;
        this.jdbcTemplate = jdbcTemplate;
        this.rabbitTemplate = rabbitTemplate;
    }

    public void processBatch(HashBatch batch) {
        UUID jobId = batch.jobId();
        List<String> hashes = batch.hashes();
        
        // Lookup hashes in database
        Map<String, String> found = lookupService.lookupBatch(hashes);
        
        // Insert results into database (idempotent)
        if (!found.isEmpty()) {
            String sql = "INSERT INTO results (job_id, hash_hex, phone_number, found_at) " +
                        "VALUES (?, ?, ?, now()) " +
                        "ON CONFLICT (job_id, hash_hex) DO NOTHING";
            
            List<Object[]> batchArgs = found.entrySet().stream()
                    .map(e -> new Object[]{jobId, e.getKey(), e.getValue()})
                    .collect(Collectors.toList());
            
            jdbcTemplate.batchUpdate(sql, batchArgs);
        }
        
        // Convert to ResultItem list
        List<ResultItem> resultItems = found.entrySet().stream()
                .map(e -> new ResultItem(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        
        // Publish result batch to results queue
        ResultBatch resultBatch = new ResultBatch(jobId, batch.batchIndex(), resultItems);
        rabbitTemplate.convertAndSend(
            RabbitMQConfig.EXCHANGE_NAME,
            RabbitMQConfig.RESULTS_ROUTING_KEY,
            resultBatch
        );
    }
}

