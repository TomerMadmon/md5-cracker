#!/usr/bin/env python3
"""
Load all CSV files from a directory into PostgreSQL.
This script copies files into the container and loads them.

Usage:
    python load_all_to_db.py <directory> [container_name] [db_name] [db_user]
"""

import sys
import subprocess
from pathlib import Path
import glob

def load_csv_file(csv_file: str, container: str, db_name: str, db_user: str):
    """Load a single CSV file into the database."""
    print(f"Loading {csv_file}...")
    
    # Copy file to container
    container_path = f"/tmp/{Path(csv_file).name}"
    subprocess.run([
        "docker", "cp", csv_file, f"{container}:{container_path}"
    ], check=True)
    
    # Load into staging
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", f"\\COPY staging_md5(md5_hex, phone_number) FROM '{container_path}' WITH CSV"
    ], check=True)
    
    # Insert into main table
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", """
        INSERT INTO md5_phone_map_bin (md5_hash, phone_number)
        SELECT decode(md5_hex, 'hex'), phone_number
        FROM staging_md5
        ON CONFLICT (md5_hash) DO NOTHING;
        """
    ], check=True)
    
    # Clear staging
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", "TRUNCATE staging_md5;"
    ], check=True)
    
    # Remove file from container
    subprocess.run([
        "docker", "exec", container,
        "rm", container_path
    ], check=False)
    
    print(f"✓ Loaded {csv_file}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python load_all_to_db.py <directory> [container_name] [db_name] [db_user]")
        sys.exit(1)
    
    directory = sys.argv[1]
    container = sys.argv[2] if len(sys.argv) > 2 else "md5-hash-postgres-1"
    db_name = sys.argv[3] if len(sys.argv) > 3 else "md5db"
    db_user = sys.argv[4] if len(sys.argv) > 4 else "md5"
    
    csv_files = sorted(glob.glob(str(Path(directory) / "*.csv")))
    
    if not csv_files:
        print(f"No CSV files found in {directory}")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV files to load")
    print(f"Container: {container}")
    print(f"Database: {db_name}")
    print(f"User: {db_user}")
    print()
    
    for i, csv_file in enumerate(csv_files, 1):
        try:
            load_csv_file(csv_file, container, db_name, db_user)
            print(f"Progress: {i}/{len(csv_files)}\n")
        except subprocess.CalledProcessError as e:
            print(f"Error loading {csv_file}: {e}")
            sys.exit(1)
    
    # Analyze table for better query performance
    print("Running ANALYZE on md5_phone_map_bin...")
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", "ANALYZE md5_phone_map_bin;"
    ], check=True)
    
    # Show count
    print("\nFinal count:")
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", "SELECT COUNT(*) FROM md5_phone_map_bin;"
    ], check=True)
    
    print("\nDone!")

if __name__ == '__main__':
    main()

