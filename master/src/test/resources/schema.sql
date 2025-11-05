-- Jobs table (using VARCHAR for UUID in H2)
CREATE TABLE IF NOT EXISTS jobs (
  job_id VARCHAR(36) PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
  total_hashes INT,
  batches_expected INT,
  batches_completed INT DEFAULT 0,
  found_count INT DEFAULT 0
);

-- Targets table to preserve original input hashes
CREATE TABLE IF NOT EXISTS targets (
  job_id VARCHAR(36),
  hash_hex CHAR(32) NOT NULL,
  PRIMARY KEY (job_id, hash_hex),
  FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_targets_job_id ON targets(job_id);

-- Results table (minion inserts)
CREATE TABLE IF NOT EXISTS results (
  job_id VARCHAR(36),
  hash_hex CHAR(32) NOT NULL,
  phone_number CHAR(11),
  found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (job_id, hash_hex),
  FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);

-- Precomputed MD5 phone map table (for H2 we use VARBINARY instead of BYTEA)
CREATE TABLE IF NOT EXISTS md5_phone_map_bin (
  md5_hash VARBINARY(16) PRIMARY KEY,
  phone_number CHAR(11) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_md5_phone_map_phone ON md5_phone_map_bin(phone_number);

-- Staging table for bulk loading
CREATE TABLE IF NOT EXISTS staging_md5 (
  md5_hex CHAR(32),
  phone_number CHAR(11)
);

