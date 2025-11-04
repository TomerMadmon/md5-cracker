package com.md5cracker.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class HashLookupService {
    
    private final JdbcTemplate jdbcTemplate;

    public HashLookupService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Lookup MD5 hashes in the database using a safe IN clause approach.
     * This is simpler and works well for batches up to 2000 items.
     */
    public Map<String, String> lookupBatch(List<String> hashesHex) {
        if (hashesHex.isEmpty()) {
            return Map.of();
        }

        // Use the simpler IN clause method for better reliability
        return lookupBatchSimple(hashesHex);
    }

    /**
     * Simple and reliable approach using IN clause with decode (for batches < 2000)
     */
    public Map<String, String> lookupBatchSimple(List<String> hashesHex) {
        if (hashesHex.isEmpty()) {
            return Map.of();
        }

        // Build IN clause with decode
        String inClause = hashesHex.stream()
                .map(h -> "decode(?, 'hex')")
                .collect(Collectors.joining(","));

        String sql = "SELECT encode(md5_hash, 'hex') as md5_hex, phone_number " +
                "FROM md5_phone_map_bin " +
                "WHERE md5_hash IN (" + inClause + ")";

        Object[] params = hashesHex.toArray();

        Map<String, String> results = new HashMap<>();
        jdbcTemplate.query(sql, params, (rs) -> {
            while (rs.next()) {
                String hashHex = rs.getString("md5_hex");
                String phone = rs.getString("phone_number");
                results.put(hashHex, phone);
            }
            return results;
        });

        return results;
    }
}

