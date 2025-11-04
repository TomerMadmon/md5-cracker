#!/usr/bin/env python3
"""
Generate precomputed MD5 hash CSV files for phone number ranges.

Usage:
    python generate_precomp.py --prefix 050 --start 1000000 --count 10000000 --out precomp_050.csv
    python generate_precomp.py --prefix 050 --parallel 10 --out-dir ./precomp_data
"""

import argparse
import hashlib
import csv
import multiprocessing
from pathlib import Path
from typing import Generator


def generate_phones(prefix: str, start: int, count: int, with_dash: bool = True) -> Generator[tuple[str, str], None, None]:
    """Generate phone numbers and their MD5 hashes.
    
    Args:
        prefix: Phone prefix (e.g., '050')
        start: Starting number (0-9999999)
        count: Number of phones to generate
        with_dash: If True, format as 05X-XXXXXXX, else 05XXXXXXXXX
    """
    for i in range(start, start + count):
        if with_dash:
            phone = f"{prefix}-{i:07d}"  # 050-1234567 = 11 chars
        else:
            phone = f"{prefix}{i:08d}"  # 05012345678 = 11 chars
        md5_hash = hashlib.md5(phone.encode()).hexdigest()
        yield (md5_hash, phone)


def write_csv(prefix: str, start: int, count: int, output_file: str, with_dash: bool = True):
    """Write a single CSV file with MD5 hashes and phone numbers."""
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for md5_hex, phone in generate_phones(prefix, start, count, with_dash):
            writer.writerow([md5_hex, phone])


def worker(args):
    """Worker function for parallel generation."""
    prefix, start, count, output_file, with_dash = args
    write_csv(prefix, start, count, output_file, with_dash)
    return output_file


def generate_parallel(prefix: str, total_count: int, num_files: int, out_dir: str, with_dash: bool = True):
    """Generate multiple CSV files in parallel."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    count_per_file = total_count // num_files
    
    tasks = []
    for i in range(num_files):
        start = i * count_per_file
        count = count_per_file if i < num_files - 1 else total_count - start
        output_file = str(Path(out_dir) / f"precomp_{prefix}_part{i:03d}.csv")
        tasks.append((prefix, start, count, output_file, with_dash))
    
    with multiprocessing.Pool(processes=num_files) as pool:
        results = pool.map(worker, tasks)
    
    print(f"Generated {len(results)} files in {out_dir}")
    return results


def main():
    parser = argparse.ArgumentParser(description='Generate precomputed MD5 hash CSV files')
    parser.add_argument('--prefix', type=str, required=True, help='Phone prefix (e.g., 050)')
    parser.add_argument('--start', type=int, default=0, help='Starting phone number (default: 0)')
    parser.add_argument('--count', type=int, help='Number of phone numbers to generate')
    parser.add_argument('--out', type=str, help='Output CSV file path')
    parser.add_argument('--parallel', type=int, help='Number of parallel files to generate')
    parser.add_argument('--out-dir', type=str, default='./precomp_data', help='Output directory for parallel generation')
    parser.add_argument('--no-dash', action='store_true', help='Generate without dash (05XXXXXXXXX format)')
    
    args = parser.parse_args()
    with_dash = not args.no_dash
    
    if args.parallel:
        if not args.count:
            print("Error: --count is required when using --parallel")
            return
        generate_parallel(args.prefix, args.count, args.parallel, args.out_dir, with_dash)
    else:
        if not args.count or not args.out:
            print("Error: --count and --out are required when not using --parallel")
            return
        write_csv(args.prefix, args.start, args.count, args.out, with_dash)
        print(f"Generated {args.count} rows in {args.out}")


if __name__ == '__main__':
    main()

