"""
Data generation script — CLI entry point.
Run: python scripts/generate_data.py --records 50000
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse

from src.ingestion.data_generator import run_generation


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce data for the demand-supply system."
    )
    parser.add_argument(
        "--records",
        type=int,
        default=50_000,
        help="Number of order records to generate (default: 50,000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./data/raw",
        help="Output directory for generated files (default: ./data/raw)",
    )
    args = parser.parse_args()

    datasets = run_generation(output_dir=args.output, n_orders=args.records)
    print("\n✅ Datasets generated:")
    for name, df in datasets.items():
        print(f"   {name:20s}: {len(df):>8,} rows")


if __name__ == "__main__":
    main()
