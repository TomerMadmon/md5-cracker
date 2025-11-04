package com.md5cracker.dto;

import java.util.List;
import java.util.UUID;

public record ResultBatch(UUID jobId, int batchIndex, List<ResultItem> results) {
}

