# Quick Start - Load All Phone Numbers

## One Command to Load Everything

To generate and load all 100 million phone numbers (05X-XXXXXXX format):

```bash
cd db/load
python load_all_numbers.py
```

This will:
1. ✅ Generate all prefixes 050-059 (10 million phones each)
2. ✅ Load all data into PostgreSQL
3. ✅ Show progress and statistics
4. ✅ Run ANALYZE for optimal performance

## Options

```bash
# Skip generation if CSV files already exist
python load_all_numbers.py --skip-generation

# Use custom output directory
python load_all_numbers.py --out-dir /path/to/data

# Adjust parallel generation (more = faster but uses more CPU/RAM)
python load_all_numbers.py --parallel 20

# Custom container/database settings
python load_all_numbers.py --container my-postgres --db-name mydb --db-user myuser
```

## What Gets Generated

- **10 prefixes**: 050, 051, 052, 053, 054, 055, 056, 057, 058, 059
- **10 million phones per prefix**: 0000000-9999999
- **Total**: 100 million phone numbers
- **Format**: `05X-XXXXXXX` (e.g., `050-1234567`)

## Time Estimates

- **Generation**: ~30-60 minutes (depends on CPU)
- **Loading**: ~1-2 hours (depends on disk I/O)
- **Total**: ~2-3 hours for complete 100M row load

## Storage Requirements

- **CSV files**: ~5-6 GB
- **Database table**: ~8-10 GB (including indexes)
- **Total disk space needed**: ~15-20 GB

## Check Progress

While the script is running, you can check the database:

```bash
# Check current row count
docker compose exec postgres psql -U md5 -d md5db -c "SELECT COUNT(*) FROM md5_phone_map_bin;"

# Check table size
docker compose exec postgres psql -U md5 -d md5db -c "SELECT pg_size_pretty(pg_total_relation_size('md5_phone_map_bin'));"
```

## Troubleshooting

### Container not found
```bash
docker compose up -d postgres
```

### Out of disk space
- Generate in smaller batches using `--parallel 5`
- Load prefixes one at a time

### Script stops/crashes
- Use `--skip-generation` to resume loading from existing CSV files
- The script handles duplicates safely (ON CONFLICT DO NOTHING)

