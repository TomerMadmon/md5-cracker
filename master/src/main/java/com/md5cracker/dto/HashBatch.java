package com.md5cracker.dto;

import java.util.List;
import java.util.UUID;

public record HashBatch(UUID jobId, int batchIndex, List<String> hashes) {
}

