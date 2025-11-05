#!/usr/bin/env python3
"""
Complete script to generate and load all phone numbers (05X-XXXXXXX format) into PostgreSQL.

This script will:
1. Generate all prefixes 050-059, each with 10 million phone numbers (100M total)
2. Load all generated CSV files into PostgreSQL in parallel
3. Track loading state to resume from failures
4. Show progress and statistics

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
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Set

# Try to import psycopg2 for direct DB connection (when running in Docker)
try:
    import psycopg2
    from psycopg2.extras import execute_values
    from psycopg2 import extensions
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    extensions = None

def check_docker_container(container_name: str) -> bool:
    """Check if Docker container is running."""
    # Skip check if running inside Docker (docker command won't be available)
    if os.getenv('POSTGRES_HOST'):
        return True  # Assume container is running if we're in Docker
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        return container_name in result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
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
    
    # Otherwise use docker exec (only if not in Docker)
    if db_host:
        # We're in Docker but psycopg2 failed, return False to be safe
        return False
    try:
        result = subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name, "-t",
            "-c", "SELECT COUNT(*) FROM md5_phone_map_bin;"
        ], capture_output=True, text=True, check=True)
        count = int(result.stdout.strip())
        return count == 0
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
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
    
    # Otherwise use docker exec (only if not in Docker)
    if db_host:
        print("❌ Cannot clear table: psycopg2 connection failed")
        return False
    try:
        print("Clearing md5_phone_map_bin table...")
        subprocess.run([
            "docker", "exec", container,
            "psql", "-U", db_user, "-d", db_name,
            "-c", "TRUNCATE TABLE md5_phone_map_bin CASCADE;"
        ], check=True, capture_output=True)
        print("✓ Table cleared")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
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
    
    file_name = Path(csv_file).name
    max_retries = 3
    retry_delay = 0.1  # Start with 100ms
    
    for attempt in range(max_retries):
        try:
            file_start = time.time()
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            # Set a lock timeout to avoid hanging
            conn.set_session(isolation_level=extensions.ISOLATION_LEVEL_READ_COMMITTED)
            cur = conn.cursor()
            cur.execute("SET lock_timeout = '5s'")
            
            # Clear staging table first (use DELETE instead of TRUNCATE to avoid deadlocks)
            # DELETE is safer for concurrent operations
            cur.execute("DELETE FROM staging_md5")
            
            # Read CSV and insert in batches
            batch_size = 10000
            batch = []
            total_rows = 0
            batch_count = 0
            
            if attempt == 0:  # Only print on first attempt
                print(f"  [LOADING] {file_name}: Reading CSV file...")
            
            with open(csv_file, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    md5_hex, phone = row
                    batch.append((md5_hex, phone))
                    total_rows += 1
                    
                    if len(batch) >= batch_size:
                        # Insert batch (staging table has no constraints, so no ON CONFLICT needed)
                        execute_values(
                            cur,
                            "INSERT INTO staging_md5 (md5_hex, phone_number) VALUES %s",
                            batch
                        )
                        batch_count += 1
                        batch = []
                        if batch_count % 10 == 0 and attempt == 0:
                            print(f"  [LOADING] {file_name}: Loaded {batch_count * batch_size:,} rows into staging...")
                
                # Insert remaining
                if batch:
                    execute_values(
                        cur,
                        "INSERT INTO staging_md5 (md5_hex, phone_number) VALUES %s",
                        batch
                    )
                    batch_count += 1
            
            if attempt == 0:
                print(f"  [LOADING] {file_name}: Loaded {total_rows:,} rows into staging. Moving to main table...")
            
            # Insert into main table
            cur.execute("""
                INSERT INTO md5_phone_map_bin (md5_hash, phone_number)
                SELECT decode(md5_hex, 'hex'), phone_number
                FROM staging_md5
                ON CONFLICT (md5_hash) DO NOTHING
            """)
            inserted_count = cur.rowcount if hasattr(cur, 'rowcount') else 0
            
            # Clear staging - use DELETE instead of TRUNCATE to avoid deadlocks
            cur.execute("DELETE FROM staging_md5")
            
            conn.commit()
            cur.close()
            conn.close()
            
            file_time = time.time() - file_start
            if attempt > 0:
                print(f"  [DONE] {file_name}: {total_rows:,} rows processed in {file_time:.1f}s (retry {attempt})")
            else:
                print(f"  [DONE] {file_name}: {total_rows:,} rows processed in {file_time:.1f}s")
            
            return True
            
        except extensions.QueryCanceledError:
            # Lock timeout occurred
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                print(f"  [RETRY] {file_name}: Lock timeout, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  [ERROR] {file_name}: Lock timeout after {max_retries} attempts")
                return False
                
        except (extensions.TransactionRollbackError, psycopg2.OperationalError) as e:
            error_str = str(e).lower()
            if 'deadlock' in error_str or 'lock' in error_str:
                # Deadlock detected
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"  [RETRY] {file_name}: Deadlock detected, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  [ERROR] {file_name}: Deadlock after {max_retries} attempts: {e}")
                    return False
                
        except Exception as e:
            print(f"  [ERROR] {file_name}: {e}")
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                print(f"  [RETRY] {file_name}: Retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            return False
    
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

def get_state_file(directory: str) -> Path:
    """Get path to state file."""
    return Path(directory) / ".load_state.json"

def load_state(directory: str) -> Dict:
    """Load state from file."""
    state_file = get_state_file(directory)
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Warning: Could not read state file: {e}")
    return {
        "loaded_files": [],
        "failed_files": [],
        "start_time": None,
        "last_update": None
    }

def save_state(directory: str, state: Dict):
    """Save state to file."""
    state_file = get_state_file(directory)
    state["last_update"] = time.time()
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"⚠️  Warning: Could not save state file: {e}")

def load_csv_file_worker(args_tuple):
    """Worker function for parallel loading (must be at module level for multiprocessing)."""
    csv_file, container, db_name, db_user, db_host, db_port, db_password = args_tuple
    return load_csv_file(csv_file, container, db_name, db_user, db_host, db_port, db_password)

def load_all_files(directory: str, container: str, db_name: str, db_user: str,
                   db_host: str = None, db_port: int = None, db_password: str = None,
                   max_workers: int = 4):
    """Load all CSV files from directory into database in parallel with state tracking."""
    csv_files = sorted(glob.glob(str(Path(directory) / "*.csv")))
    
    if not csv_files:
        print(f"❌ No CSV files found in {directory}")
        return False
    
    # Load state
    state = load_state(directory)
    loaded_files_set = set(state.get("loaded_files", []))
    failed_files_set = set(state.get("failed_files", []))
    
    # Filter out already loaded files
    files_to_load = [f for f in csv_files if str(Path(f).name) not in loaded_files_set]
    
    if not files_to_load:
        print("=" * 70)
        print("DATABASE LOADING")
        print("=" * 70)
        print(f"✓ All {len(csv_files)} files have already been loaded")
        print("Use --reset-state to reload all files")
        return True
    
    print("=" * 70)
    print("DATABASE LOADING")
    print("=" * 70)
    print(f"Total CSV files: {len(csv_files)}")
    print(f"Already loaded: {len(loaded_files_set)}")
    print(f"Files to load: {len(files_to_load)}")
    print(f"Parallel workers: {max_workers}")
    if db_host:
        print(f"Connecting to: {db_host}:{db_port or 5432}/{db_name}")
    else:
        print(f"Container: {container}")
        print(f"Database: {db_name}")
    print()
    print("Starting to load CSV files into database...")
    print("Each file contains ~1,000,000 MD5 hash → phone number mappings")
    print("=" * 70)
    print()
    
    # Initialize state if starting fresh
    if state.get("start_time") is None:
        state["start_time"] = time.time()
        save_state(directory, state)
    
    start_time = time.time()
    loaded_count = len(loaded_files_set)
    failed_count = len(failed_files_set)
    newly_loaded = 0
    newly_failed = 0
    
    # Prepare worker arguments
    worker_args = [
        (csv_file, container, db_name, db_user, db_host, db_port, db_password)
        for csv_file in files_to_load
    ]
    
    # Use ProcessPoolExecutor for parallel loading
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(load_csv_file_worker, args): Path(args[0]).name
            for args in worker_args
        }
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_file):
            file_name = future_to_file[future]
            completed += 1
            
            try:
                success = future.result()
                file_time = time.time() - start_time
                
                if success:
                    newly_loaded += 1
                    loaded_count += 1
                    state["loaded_files"].append(file_name)
                    # Remove from failed if it was there
                    if file_name in state["failed_files"]:
                        state["failed_files"].remove(file_name)
                    
                    # Show progress after each file
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(files_to_load) - completed) / rate if rate > 0 else 0
                    progress_pct = (completed / len(files_to_load) * 100) if len(files_to_load) > 0 else 0
                    
                    print(f"[{completed}/{len(files_to_load)}] ({progress_pct:.1f}%) ✓ {file_name} | "
                          f"Loaded: {newly_loaded} | Failed: {newly_failed} | "
                          f"Elapsed: {elapsed/60:.1f}m | Remaining: {remaining/60:.1f}m")
                else:
                    newly_failed += 1
                    failed_count += 1
                    if file_name not in state["failed_files"]:
                        state["failed_files"].append(file_name)
                    
                    elapsed = time.time() - start_time
                    progress_pct = (completed / len(files_to_load) * 100) if len(files_to_load) > 0 else 0
                    print(f"[{completed}/{len(files_to_load)}] ({progress_pct:.1f}%) ❌ {file_name} | "
                          f"Failed: {newly_failed}")
                
                # Save state periodically (every 5 files)
                if completed % 5 == 0:
                    save_state(directory, state)
                    
                    # Show summary progress
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(files_to_load) - completed) / rate if rate > 0 else 0
                    print(f"  ════════════════════════════════════════════════════════════════════════")
                    print(f"  📊 SUMMARY: {completed}/{len(files_to_load)} files processed | "
                          f"✅ Loaded: {newly_loaded} | ❌ Failed: {newly_failed} | "
                          f"⏱️  Elapsed: {elapsed/60:.1f}m | ⏳ Remaining: {remaining/60:.1f}m")
                    print(f"  ════════════════════════════════════════════════════════════════════════")
                    
            except Exception as e:
                newly_failed += 1
                failed_count += 1
                if file_name not in state["failed_files"]:
                    state["failed_files"].append(file_name)
                print(f"[{completed}/{len(files_to_load)}] ❌ {file_name} (Exception: {e})")
    
    # Final state save
    save_state(directory, state)
    
    total_time = time.time() - start_time
    
    print()
    print(f"✓ Loaded {newly_loaded} new files (total: {loaded_count}/{len(csv_files)})")
    if newly_failed > 0:
        print(f"❌ Failed to load {newly_failed} files (total failed: {failed_count})")
    
    print(f"✓ Total loading time: {total_time/60:.1f} minutes")
    print()
    
    # Analyze table for better query performance (only if all files loaded)
    if len(files_to_load) == newly_loaded:
        print("Running ANALYZE on md5_phone_map_bin...")
        try:
            # Always use direct connection when running in Docker (has db_host and db_password)
            # Check if we have connection info from environment (Docker mode)
            if db_host and db_password:
                if HAS_PSYCOPG2:
                    conn = psycopg2.connect(
                        host=db_host,
                        port=db_port or 5432,
                        database=db_name,
                        user=db_user,
                        password=db_password
                    )
                    cur = conn.cursor()
                    cur.execute("ANALYZE md5_phone_map_bin;")
                    conn.commit()
                    cur.close()
                    conn.close()
                    print("✓ ANALYZE completed")
                else:
                    print("⚠ Warning: psycopg2 not available, skipping ANALYZE")
            else:
                # Only use docker exec when running from host (not in Docker)
                # This should not happen when running in Docker Compose
                try:
                    subprocess.run([
                        "docker", "exec", container,
                        "psql", "-U", db_user, "-d", db_name,
                        "-c", "ANALYZE md5_phone_map_bin;"
                    ], check=True, capture_output=True)
                    print("✓ ANALYZE completed")
                except FileNotFoundError:
                    print("⚠ Warning: docker command not available, skipping ANALYZE")
        except Exception as e:
            print(f"⚠ Warning: ANALYZE failed: {e}")
        
        # Show final statistics
        print()
        print("Final statistics:")
        try:
            # Always use direct connection when running in Docker
            if HAS_PSYCOPG2 and db_host and db_password:
                conn = psycopg2.connect(
                    host=db_host,
                    port=db_port or 5432,
                    database=db_name,
                    user=db_user,
                    password=db_password
                )
                cur = conn.cursor()
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_rows,
                        pg_size_pretty(pg_total_relation_size('md5_phone_map_bin')) as table_size
                    FROM md5_phone_map_bin;
                """)
                result = cur.fetchone()
                print(f"Total rows: {result[0]:,}")
                print(f"Table size: {result[1]}")
                cur.close()
                conn.close()
            else:
                # Only use docker exec when running from host (not in Docker)
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
                except (subprocess.CalledProcessError, FileNotFoundError) as e:
                    # If docker exec fails and we're in Docker, try direct connection
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
                            cur.execute("""
                                SELECT 
                                    COUNT(*) as total_rows,
                                    pg_size_pretty(pg_total_relation_size('md5_phone_map_bin')) as table_size
                                FROM md5_phone_map_bin;
                            """)
                            result = cur.fetchone()
                            print(f"Total rows: {result[0]:,}")
                            print(f"Table size: {result[1]}")
                            cur.close()
                            conn.close()
                        except Exception as e2:
                            print(f"⚠ Warning: Could not get statistics: {e2}")
                    else:
                        print(f"⚠ Warning: Could not get statistics: {e}")
        except Exception as e:
            print(f"⚠ Warning: Could not get statistics: {e}")
    
    return newly_failed == 0

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
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Number of parallel workers for loading (default: 4)')
    parser.add_argument('--reset-state', action='store_true',
                       help='Reset loading state and start from beginning')
    
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
    
    # Reset state if requested
    if args.reset_state:
        state_file = get_state_file(args.out_dir)
        if state_file.exists():
            state_file.unlink()
            print("✓ Loading state reset")
        print()
    
    # Check if table is empty or needs clearing
    is_empty = check_table_empty(args.container, args.db_name, args.db_user, db_host, db_port, db_password)
    if not is_empty:
        if args.clear_table:
            if not clear_table(args.container, args.db_name, args.db_user, db_host, db_port, db_password):
                print("❌ Failed to clear table")
                sys.exit(1)
            # Also reset state when clearing table
            state_file = get_state_file(args.out_dir)
            if state_file.exists():
                state_file.unlink()
                print("✓ Loading state reset")
        else:
            print("⚠️  Warning: md5_phone_map_bin table is not empty!")
            print("   Use --clear-table to clear it before loading")
            print("   Or manually clear: TRUNCATE TABLE md5_phone_map_bin;")
            print("   Note: Script will resume loading from state file if it exists")
            response = input("   Continue anyway? (y/N): ")
            if response.lower() != 'y':
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
                         db_host, db_port, db_password, args.max_workers):
        print("❌ Some files failed to load")
        print("   You can rerun the script to retry failed files")
        state = load_state(args.out_dir)
        if state.get("failed_files"):
            print(f"   Failed files: {len(state['failed_files'])}")
        sys.exit(1)
    
    print()
    print("=" * 70)
    print("✓ ALL DONE!")
    print("=" * 70)
    print("All phone numbers have been loaded into md5_phone_map_bin table")
    print("You can now use the system to lookup MD5 hashes.")

if __name__ == '__main__':
    main()

