# Data Loading Guide

## Format

Phone numbers are generated in the format: **05X-XXXXXXX** (e.g., `050-1234567`)

## Quick Start - Generate All Prefixes (100M rows)

Generate all prefixes 050-059, each with 10 million phone numbers:

```bash
cd db/load
python generate_all_prefixes.py --out-dir ./precomp_data --parallel 10
```

This will:
- Generate 10 prefixes (050-059)
- 10 million phone numbers per prefix
- Total: 100 million phone numbers
- Split into parallel files for faster generation

## Load Data into PostgreSQL

Once CSV files are generated, load them:

```bash
python load_all_to_db.py ./precomp_data
```

This will:
- Copy each CSV into the PostgreSQL container
- Load into staging table
- Insert into md5_phone_map_bin (convert hex to bytea)
- Run ANALYZE for query optimization

## Generate Single Prefix

For testing or smaller datasets:

```bash
# Generate 10K phones for prefix 050
python generate_precomp.py --prefix 050 --start 0 --count 10000 --out precomp_050_test.csv

# Generate 10M phones in parallel (10 files)
python generate_precomp.py --prefix 050 --count 10000000 --parallel 10 --out-dir ./precomp_data
```

## Format Options

By default, phone numbers are generated with dash format: `050-1234567`

To generate without dash (05XXXXXXXXX format):
```bash
python generate_precomp.py --prefix 050 --count 1000 --out test.csv --no-dash
```

## Performance Tips

- Use `--parallel` for large datasets (10-20 parallel files)
- Each prefix has 10M possible numbers (0000000-9999999)
- Total for all 10 prefixes: 100M rows
- Loading time depends on disk I/O (~1-2 hours for 100M rows)

## Storage Estimates

- 100M rows ≈ 8-10 GB (including indexes)
- Each phone number: 11 chars + 16 bytes MD5 hash = ~27 bytes/row
- Index size: ~2-3 GB

