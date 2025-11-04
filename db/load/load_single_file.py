#!/usr/bin/env python3
"""
Load a single CSV file into PostgreSQL.

Usage:
    python load_single_file.py <csv_file> [container_name] [db_name] [db_user]
"""

import sys
import subprocess
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print("Usage: python load_single_file.py <csv_file> [container_name] [db_name] [db_user]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    container = sys.argv[2] if len(sys.argv) > 2 else "md5-hash-postgres-1"
    db_name = sys.argv[3] if len(sys.argv) > 3 else "md5db"
    db_user = sys.argv[4] if len(sys.argv) > 4 else "md5"
    
    if not Path(csv_file).exists():
        print(f"Error: File {csv_file} not found")
        sys.exit(1)
    
    print(f"Loading {csv_file} into {container}/{db_name}...")
    
    # Copy file to container
    container_path = f"/tmp/{Path(csv_file).name}"
    print(f"Copying file to container...")
    subprocess.run([
        "docker", "cp", csv_file, f"{container}:{container_path}"
    ], check=True)
    
    # Load into staging
    print("Loading into staging table...")
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", f"\\COPY staging_md5(md5_hex, phone_number) FROM '{container_path}' WITH CSV"
    ], check=True)
    
    # Insert into main table
    print("Inserting into md5_phone_map_bin...")
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
    print("Clearing staging table...")
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
    
    # Show count
    print("\nCurrent count in md5_phone_map_bin:")
    subprocess.run([
        "docker", "exec", container,
        "psql", "-U", db_user, "-d", db_name,
        "-c", "SELECT COUNT(*) FROM md5_phone_map_bin;"
    ], check=True)
    
    print("\n✓ Done!")

if __name__ == '__main__':
    main()

