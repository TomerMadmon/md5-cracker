-- Jobs table
CREATE TABLE IF NOT EXISTS jobs (
  job_id uuid PRIMARY KEY,
  created_at timestamptz DEFAULT now(),
  status text NOT NULL DEFAULT 'RUNNING',
  total_hashes int,
  batches_expected int,
  batches_completed int DEFAULT 0,
  found_count int DEFAULT 0
);

-- Targets table to preserve original input hashes
CREATE TABLE IF NOT EXISTS targets (
  job_id uuid REFERENCES jobs(job_id) ON DELETE CASCADE,
  hash_hex CHAR(32) NOT NULL,
  PRIMARY KEY (job_id, hash_hex)
);

CREATE INDEX IF NOT EXISTS idx_targets_job_id ON targets(job_id);

-- Results table (minion inserts)
CREATE TABLE IF NOT EXISTS results (
  job_id uuid REFERENCES jobs(job_id) ON DELETE CASCADE,
  hash_hex CHAR(32) NOT NULL,
  phone_number CHAR(11),
  found_at timestamptz DEFAULT now(),
  PRIMARY KEY (job_id, hash_hex)
);

CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);

-- Precomputed MD5 phone map table (16-byte binary)
-- Option 1: Single table (simpler, good for < 500M rows)
CREATE TABLE IF NOT EXISTS md5_phone_map_bin (
  md5_hash BYTEA PRIMARY KEY,
  phone_number CHAR(11) NOT NULL
);

-- Option 2: Partitioned table (recommended for 100M+ rows)
-- Uncomment below if you want partitioning by phone prefix
/*
CREATE TABLE IF NOT EXISTS md5_phone_map_bin (
  md5_hash BYTEA NOT NULL,
  phone_number CHAR(11) NOT NULL,
  phone_prefix CHAR(3) GENERATED ALWAYS AS (substring(phone_number, 1, 3)) STORED
) PARTITION BY LIST (phone_prefix);

CREATE TABLE IF NOT EXISTS md5_phone_map_bin_050 PARTITION OF md5_phone_map_bin FOR VALUES IN ('050');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_051 PARTITION OF md5_phone_map_bin FOR VALUES IN ('051');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_052 PARTITION OF md5_phone_map_bin FOR VALUES IN ('052');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_053 PARTITION OF md5_phone_map_bin FOR VALUES IN ('053');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_054 PARTITION OF md5_phone_map_bin FOR VALUES IN ('054');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_055 PARTITION OF md5_phone_map_bin FOR VALUES IN ('055');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_056 PARTITION OF md5_phone_map_bin FOR VALUES IN ('056');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_057 PARTITION OF md5_phone_map_bin FOR VALUES IN ('057');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_058 PARTITION OF md5_phone_map_bin FOR VALUES IN ('058');
CREATE TABLE IF NOT EXISTS md5_phone_map_bin_059 PARTITION OF md5_phone_map_bin FOR VALUES IN ('059');

ALTER TABLE md5_phone_map_bin ADD PRIMARY KEY (md5_hash);
*/

-- Staging table for bulk loading
CREATE UNLOGGED TABLE IF NOT EXISTS staging_md5 (
  md5_hex CHAR(32),
  phone_number CHAR(11)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_md5_phone_map_phone ON md5_phone_map_bin(phone_number);

-- Analyze after loading
ANALYZE md5_phone_map_bin;

