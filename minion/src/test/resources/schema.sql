-- Precomputed MD5 phone map table (for H2 we use VARBINARY instead of BYTEA)
CREATE TABLE IF NOT EXISTS md5_phone_map_bin (
  md5_hash VARBINARY(16) PRIMARY KEY,
  phone_number CHAR(11) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_md5_phone_map_phone ON md5_phone_map_bin(phone_number);

-- Results table (minion inserts)
CREATE TABLE IF NOT EXISTS results (
  job_id VARCHAR(36),
  hash_hex CHAR(32) NOT NULL,
  phone_number CHAR(11),
  found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (job_id, hash_hex)
);

CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);

