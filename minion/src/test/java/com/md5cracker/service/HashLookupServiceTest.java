package com.md5cracker.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HashLookupServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private HashLookupService hashLookupService;

    private String hash1;
    private String hash2;
    private String hash3;

    @BeforeEach
    void setUp() {
        hash1 = "a1b2c3d4e5f6789012345678901234ab";
        hash2 = "1234567890abcdef1234567890abcdef";
        hash3 = "fedcba0987654321fedcba0987654321";
    }

    @Test
    void lookupBatch_WithEmptyList_ReturnsEmptyMap() {
        // Act
        Map<String, String> result = hashLookupService.lookupBatch(List.of());

        // Assert
        assertTrue(result.isEmpty());
        verify(jdbcTemplate, never()).query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }

    @Test
    void lookupBatch_WithSingleHash_FindsMatch() {
        // Arrange
        String phone = "1234567890";
        
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(true, false);
                when(rs.getString("md5_hex")).thenReturn(hash1);
                when(rs.getString("phone_number")).thenReturn(phone);
                
                @SuppressWarnings("unchecked")
                org.springframework.jdbc.core.ResultSetExtractor<Map<String, String>> callback = 
                    invocation.getArgument(2, org.springframework.jdbc.core.ResultSetExtractor.class);
                return callback.extractData(rs);
            });

        // Act
        Map<String, String> result = hashLookupService.lookupBatch(Arrays.asList(hash1));

        // Assert
        assertEquals(1, result.size());
        assertEquals(phone, result.get(hash1));
        verify(jdbcTemplate, times(1)).query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }

    @Test
    void lookupBatch_WithMultipleHashes_FindsMatches() {
        // Arrange
        String phone1 = "1234567890";
        String phone2 = "9876543210";
        
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(true, true, false);
                when(rs.getString("md5_hex")).thenReturn(hash1, hash2);
                when(rs.getString("phone_number")).thenReturn(phone1, phone2);
                
                @SuppressWarnings("unchecked")
                org.springframework.jdbc.core.ResultSetExtractor<Map<String, String>> callback = 
                    invocation.getArgument(2, org.springframework.jdbc.core.ResultSetExtractor.class);
                return callback.extractData(rs);
            });

        // Act
        Map<String, String> result = hashLookupService.lookupBatch(Arrays.asList(hash1, hash2, hash3));

        // Assert
        assertEquals(2, result.size());
        assertEquals(phone1, result.get(hash1));
        assertEquals(phone2, result.get(hash2));
        assertNull(result.get(hash3));
    }

    @Test
    void lookupBatch_WithNoMatches_ReturnsEmptyMap() {
        // Arrange
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(false);
                
                @SuppressWarnings("unchecked")
                org.springframework.jdbc.core.ResultSetExtractor<Map<String, String>> callback = 
                    invocation.getArgument(2, org.springframework.jdbc.core.ResultSetExtractor.class);
                return callback.extractData(rs);
            });

        // Act
        Map<String, String> result = hashLookupService.lookupBatch(Arrays.asList(hash1, hash2));

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void lookupBatch_BuildsCorrectSqlQuery() {
        // Arrange
        List<String> hashes = Arrays.asList(hash1, hash2);
        
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                String sql = invocation.getArgument(0, String.class);
                // Verify SQL contains decode and IN clause
                assertTrue(sql.contains("decode"));
                assertTrue(sql.contains("IN"));
                assertTrue(sql.contains("md5_phone_map_bin"));
                
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(false);
                @SuppressWarnings("unchecked")
                org.springframework.jdbc.core.ResultSetExtractor<Map<String, String>> callback = 
                    invocation.getArgument(2, org.springframework.jdbc.core.ResultSetExtractor.class);
                return callback.extractData(rs);
            });

        // Act
        hashLookupService.lookupBatch(hashes);

        // Assert - verification happens in the thenAnswer above
        verify(jdbcTemplate, times(1)).query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }

    @Test
    void lookupBatchSimple_WithEmptyList_ReturnsEmptyMap() {
        // Act
        Map<String, String> result = hashLookupService.lookupBatchSimple(List.of());

        // Assert
        assertTrue(result.isEmpty());
        verify(jdbcTemplate, never()).query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }
}

