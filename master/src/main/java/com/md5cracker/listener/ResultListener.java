package com.md5cracker.listener;

import com.md5cracker.dto.ResultBatch;
import com.md5cracker.service.JobService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class ResultListener {
    
    private final JobService jobService;

    public ResultListener(JobService jobService) {
        this.jobService = jobService;
    }

    @RabbitListener(queues = "md5.results")
    public void handleResultBatch(ResultBatch resultBatch) {
        jobService.handleResultBatch(resultBatch);
    }
}

