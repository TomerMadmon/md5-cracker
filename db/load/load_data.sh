#!/bin/bash
# Bulk load script for precomputed MD5 data
# Usage: ./load_data.sh <csv_file> <db_name> <db_user>

CSV_FILE=$1
DB_NAME=${2:-md5db}
DB_USER=${3:-md5}

if [ -z "$CSV_FILE" ]; then
    echo "Usage: ./load_data.sh <csv_file> [db_name] [db_user]"
    exit 1
fi

echo "Loading $CSV_FILE into $DB_NAME..."

# Load into staging table
psql -d "$DB_NAME" -U "$DB_USER" -c "\COPY staging_md5(md5_hex, phone_number) FROM '$CSV_FILE' WITH CSV"

# Insert into main table (convert hex to bytea)
psql -d "$DB_NAME" -U "$DB_USER" -c "
INSERT INTO md5_phone_map_bin (md5_hash, phone_number)
SELECT decode(md5_hex, 'hex'), phone_number
FROM staging_md5
ON CONFLICT (md5_hash) DO NOTHING;
"

# Clear staging table
psql -d "$DB_NAME" -U "$DB_USER" -c "TRUNCATE staging_md5;"

echo "Done loading $CSV_FILE"

