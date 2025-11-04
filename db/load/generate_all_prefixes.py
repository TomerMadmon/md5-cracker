#!/usr/bin/env python3
"""
Generate precomputed MD5 hash CSV files for all 05X prefixes (050-059).
Each prefix has 10 million phone numbers (0000000-9999999).

Usage:
    python generate_all_prefixes.py --out-dir ./precomp_data --parallel 10
"""

import subprocess
import sys
from pathlib import Path

def generate_all_prefixes(out_dir: str = "./precomp_data", parallel: int = 10):
    """Generate CSV files for all prefixes 050-059, each with 10M phone numbers."""
    prefixes = ['050', '051', '052', '053', '054', '055', '056', '057', '058', '059']
    total_phones = 10_000_000  # 10 million per prefix
    
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Generating data for {len(prefixes)} prefixes, {total_phones:,} phones each...")
    print(f"Total: {len(prefixes) * total_phones:,} phone numbers")
    print(f"Output directory: {out_dir}")
    print(f"Parallel files per prefix: {parallel}")
    print()
    
    for prefix in prefixes:
        print(f"Generating prefix {prefix}...")
        cmd = [
            sys.executable,
            "generate_precomp.py",
            "--prefix", prefix,
            "--count", str(total_phones),
            "--parallel", str(parallel),
            "--out-dir", out_dir
        ]
        result = subprocess.run(cmd, cwd=Path(__file__).parent)
        if result.returncode != 0:
            print(f"Error generating prefix {prefix}")
            return False
        print(f"✓ Completed prefix {prefix}\n")
    
    print(f"All prefixes generated in {out_dir}")
    return True

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate all 05X prefixes')
    parser.add_argument('--out-dir', type=str, default='./precomp_data', 
                       help='Output directory for CSV files')
    parser.add_argument('--parallel', type=int, default=10,
                       help='Number of parallel files per prefix')
    
    args = parser.parse_args()
    success = generate_all_prefixes(args.out_dir, args.parallel)
    sys.exit(0 if success else 1)

