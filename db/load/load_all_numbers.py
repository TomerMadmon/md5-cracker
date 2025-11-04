#!/usr/bin/env python3
"""
Complete script to generate and load all phone numbers (05X-XXXXXXX format) into PostgreSQL.

This script will:
1. Generate all prefixes 050-059, each with 10 million phone numbers (100M total)
2. Load all generated CSV files into PostgreSQL
3. Show progress and statistics

Usage:
    python load_all_numbers.py [--out-dir ./precomp_data] [--parallel 10] [--skip-generation]
"""

import sys
import subprocess
import time
import os
from pathlib import Path
import glob
import argparse

# Try to import psycopg2 for direct DB connection (when running in Docker)
try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

def check_docker_container(container_name: str) -> bool:
    """Check if Docker container is running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        return container_name in result.stdout
    except subprocess.CalledProcessError:
        return False

def check_table_empty(container: str, db_name: str, db_user: str,
                     db_host: str = None, db_port: int = None, db_password: str = None) -> bool:
    """Check if md5_phone_map_bin table is empty."""
    # If running in Docker, use direct connection
    if HAS_PSYCOPG2 and db_host and db_password:
        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port or 5432,
                database=db_name,
                user=db_user,
                password=db_password
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM md5_phone_map_bin;")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            return count == 0
        except Exception:
            return False
    
    # Otherwise use docker exec
    try:
        result = subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name, "-t",
            "-c", "SELECT COUNT(*) FROM md5_phone_map_bin;"
        ], capture_output=True, text=True, check=True)
        count = int(result.stdout.strip())
        return count == 0
    except (subprocess.CalledProcessError, ValueError):
        return False

def clear_table(container: str, db_name: str, db_user: str,
                db_host: str = None, db_port: int = None, db_password: str = None) -> bool:
    """Clear the md5_phone_map_bin table."""
    # If running in Docker, use direct connection
    if HAS_PSYCOPG2 and db_host and db_password:
        try:
            print("Clearing md5_phone_map_bin table...")
            conn = psycopg2.connect(
                host=db_host,
                port=db_port or 5432,
                database=db_name,
                user=db_user,
                password=db_password
            )
            cur = conn.cursor()
            cur.execute("TRUNCATE TABLE md5_phone_map_bin CASCADE;")
            conn.commit()
            cur.close()
            conn.close()
            print("✓ Table cleared")
            return True
        except Exception as e:
            print(f"❌ Failed to clear table: {e}")
            return False
    
    # Otherwise use docker exec
    try:
        print("Clearing md5_phone_map_bin table...")
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name,
            "-c", "TRUNCATE TABLE md5_phone_map_bin CASCADE;"
        ], check=True, capture_output=True)
        print("✓ Table cleared")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to clear table")
        return False

def generate_all_prefixes(out_dir: str, parallel: int):
    """Generate CSV files for all prefixes 050-059."""
    prefixes = ['050', '051', '052', '053', '054', '055', '056', '057', '058', '059']
    total_phones = 10_000_000  # 10 million per prefix
    
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("PHONE NUMBER GENERATION")
    print("=" * 70)
    print(f"Prefixes: {', '.join(prefixes)}")
    print(f"Phones per prefix: {total_phones:,}")
    print(f"Total phone numbers: {len(prefixes) * total_phones:,}")
    print(f"Output directory: {out_dir}")
    print(f"Parallel files per prefix: {parallel}")
    print()
    
    start_time = time.time()
    
    for i, prefix in enumerate(prefixes, 1):
        print(f"[{i}/{len(prefixes)}] Generating prefix {prefix}...")
        prefix_start = time.time()
        
        cmd = [
            sys.executable,
            str(Path(__file__).parent / "generate_precomp.py"),
            "--prefix", prefix,
            "--count", str(total_phones),
            "--parallel", str(parallel),
            "--out-dir", out_dir
        ]
        
        result = subprocess.run(cmd, cwd=Path(__file__).parent)
        if result.returncode != 0:
            print(f"❌ Error generating prefix {prefix}")
            return False
        
        prefix_time = time.time() - prefix_start
        print(f"✓ Completed prefix {prefix} in {prefix_time/60:.1f} minutes\n")
    
    total_time = time.time() - start_time
    print(f"✓ All prefixes generated in {total_time/60:.1f} minutes")
    
    # Count generated files
    csv_files = glob.glob(str(Path(out_dir) / "precomp_*.csv"))
    print(f"✓ Generated {len(csv_files)} CSV files")
    print()
    return True

def load_csv_file_direct(csv_file: str, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> bool:
    """Load CSV file directly using psycopg2 (when running in Docker)."""
    import csv
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cur = conn.cursor()
        
        # Read CSV and insert in batches
        batch_size = 10000
        batch = []
        
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                md5_hex, phone = row
                batch.append((md5_hex, phone))
                
                if len(batch) >= batch_size:
                    # Insert batch (staging table has no constraints, so no ON CONFLICT needed)
                    execute_values(
                        cur,
                        "INSERT INTO staging_md5 (md5_hex, phone_number) VALUES %s",
                        batch
                    )
                    batch = []
            
            # Insert remaining
            if batch:
                execute_values(
                    cur,
                    "INSERT INTO staging_md5 (md5_hex, phone_number) VALUES %s",
                    batch
                )
        
        # Insert into main table
        cur.execute("""
            INSERT INTO md5_phone_map_bin (md5_hash, phone_number)
            SELECT decode(md5_hex, 'hex'), phone_number
            FROM staging_md5
            ON CONFLICT (md5_hash) DO NOTHING
        """)
        
        # Clear staging
        cur.execute("TRUNCATE staging_md5")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"❌ Error loading {Path(csv_file).name}: {e}")
        return False

def load_csv_file(csv_file: str, container: str, db_name: str, db_user: str, 
                  db_host: str = None, db_port: int = None, db_password: str = None) -> bool:
    """Load a single CSV file into the database."""
    # If running in Docker (has psycopg2 and DB connection info), use direct connection
    if HAS_PSYCOPG2 and db_host and db_password:
        return load_csv_file_direct(csv_file, db_host, db_port or 5432, db_name, db_user, db_password)
    
    # Otherwise use docker exec (when running from host)
    try:
        # Copy file to container
        container_path = f"/tmp/{Path(csv_file).name}"
        abs_csv_file = str(Path(csv_file).absolute())
        subprocess.run([
            "docker", "cp", abs_csv_file, f"{container}:{container_path}"
        ], check=True, capture_output=True)
        
        # Load into staging
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name, "-q",
            "-c", f"\\COPY staging_md5(md5_hex, phone_number) FROM '{container_path}' WITH CSV"
        ], check=True, capture_output=True)
        
        # Insert into main table
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name, "-q",
            "-c", """
            INSERT INTO md5_phone_map_bin (md5_hash, phone_number)
            SELECT decode(md5_hex, 'hex'), phone_number
            FROM staging_md5
            ON CONFLICT (md5_hash) DO NOTHING;
            """
        ], check=True, capture_output=True)
        
        # Clear staging
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name, "-q",
            "-c", "TRUNCATE staging_md5;"
        ], check=True, capture_output=True)
        
        # Remove file from container
        subprocess.run([
            "docker", "exec", container,
            "rm", container_path
        ], check=False, capture_output=True)
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error loading {Path(csv_file).name}: {e}")
        return False

def load_all_files(directory: str, container: str, db_name: str, db_user: str,
                   db_host: str = None, db_port: int = None, db_password: str = None):
    """Load all CSV files from directory into database."""
    csv_files = sorted(glob.glob(str(Path(directory) / "*.csv")))
    
    if not csv_files:
        print(f"❌ No CSV files found in {directory}")
        return False
    
    print("=" * 70)
    print("DATABASE LOADING")
    print("=" * 70)
    print(f"Found {len(csv_files)} CSV files to load")
    if db_host:
        print(f"Connecting to: {db_host}:{db_port or 5432}/{db_name}")
    else:
        print(f"Container: {container}")
        print(f"Database: {db_name}")
    print()
    
    start_time = time.time()
    loaded_count = 0
    failed_count = 0
    
    for i, csv_file in enumerate(csv_files, 1):
        file_name = Path(csv_file).name
        print(f"[{i}/{len(csv_files)}] Loading {file_name}...", end=" ", flush=True)
        
        file_start = time.time()
        success = load_csv_file(csv_file, container, db_name, db_user, db_host, db_port, db_password)
        file_time = time.time() - file_start
        
        if success:
            loaded_count += 1
            print(f"✓ ({file_time:.1f}s)")
        else:
            failed_count += 1
            print(f"❌")
        
        # Show progress every 10 files
        if i % 10 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed
            remaining = (len(csv_files) - i) / rate if rate > 0 else 0
            print(f"  Progress: {i}/{len(csv_files)} files | "
                  f"Elapsed: {elapsed/60:.1f}m | "
                  f"Remaining: {remaining/60:.1f}m")
    
    total_time = time.time() - start_time
    
    print()
    print(f"✓ Loaded {loaded_count} files successfully")
    if failed_count > 0:
        print(f"❌ Failed to load {failed_count} files")
    
    print(f"✓ Total loading time: {total_time/60:.1f} minutes")
    print()
    
    # Analyze table for better query performance
    print("Running ANALYZE on md5_phone_map_bin...")
    try:
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name,
            "-c", "ANALYZE md5_phone_map_bin;"
        ], check=True, capture_output=True)
        print("✓ ANALYZE completed")
    except subprocess.CalledProcessError:
        print("⚠ Warning: ANALYZE failed")
    
    # Show final statistics
    print()
    print("Final statistics:")
    try:
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name,
            "-c", """
            SELECT 
                COUNT(*) as total_rows,
                pg_size_pretty(pg_total_relation_size('md5_phone_map_bin')) as table_size
            FROM md5_phone_map_bin;
            """
        ], check=True)
    except subprocess.CalledProcessError:
        pass
    
    return failed_count == 0

def main():
    parser = argparse.ArgumentParser(
        description='Generate and load all phone numbers (05X-XXXXXXX) into PostgreSQL'
    )
    parser.add_argument('--out-dir', type=str, default='./precomp_data',
                       help='Output directory for CSV files (default: ./precomp_data)')
    parser.add_argument('--parallel', type=int, default=10,
                       help='Number of parallel files per prefix (default: 10)')
    parser.add_argument('--skip-generation', action='store_true',
                       help='Skip generation, only load existing CSV files')
    parser.add_argument('--container', type=str, default='md5-hash-postgres-1',
                       help='PostgreSQL container name (default: md5-hash-postgres-1)')
    parser.add_argument('--db-name', type=str, default='md5db',
                       help='Database name (default: md5db)')
    parser.add_argument('--db-user', type=str, default='md5',
                       help='Database user (default: md5)')
    parser.add_argument('--clear-table', action='store_true',
                       help='Clear table before loading (default: only load if empty)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("MD5 PHONE NUMBER LOADER")
    print("=" * 70)
    print()
    
    # Get DB connection info from environment (when running in Docker)
    db_host = os.getenv('POSTGRES_HOST', None)
    db_port = int(os.getenv('POSTGRES_PORT', '5432')) if os.getenv('POSTGRES_PORT') else None
    db_password = os.getenv('DB_PASSWORD', None)
    
    # Check Docker container (skip if running in Docker with direct DB connection)
    if not (HAS_PSYCOPG2 and db_host):
        if not check_docker_container(args.container):
            print(f"❌ Error: Docker container '{args.container}' is not running")
            print("Please start the container with: docker compose up -d postgres")
            sys.exit(1)
        print(f"✓ Docker container '{args.container}' is running")
    else:
        print(f"✓ Running in Docker, connecting directly to {db_host}:{db_port or 5432}")
    print()
    
    # Check if table is empty or needs clearing
    is_empty = check_table_empty(args.container, args.db_name, args.db_user, db_host, db_port, db_password)
    if not is_empty:
        if args.clear_table:
            if not clear_table(args.container, args.db_name, args.db_user, db_host, db_port, db_password):
                print("❌ Failed to clear table")
                sys.exit(1)
        else:
            print("⚠️  Warning: md5_phone_map_bin table is not empty!")
            print("   Use --clear-table to clear it before loading")
            print("   Or manually clear: TRUNCATE TABLE md5_phone_map_bin;")
            sys.exit(1)
    else:
        print("✓ Table is empty, ready to load")
        print()
    
    # Step 1: Generate CSV files (if not skipped)
    if not args.skip_generation:
        if not generate_all_prefixes(args.out_dir, args.parallel):
            print("❌ Generation failed")
            sys.exit(1)
    else:
        print("Skipping generation (using existing files)")
        print()
    
    # Step 2: Load into database
    if not load_all_files(args.out_dir, args.container, args.db_name, args.db_user,
                         db_host, db_port, db_password):
        print("❌ Loading failed")
        sys.exit(1)
    
    print()
    print("=" * 70)
    print("✓ ALL DONE!")
    print("=" * 70)
    print("All phone numbers have been loaded into md5_phone_map_bin table")
    print("You can now use the system to lookup MD5 hashes.")

if __name__ == '__main__':
    main()

