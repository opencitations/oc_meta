#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import argparse
import csv
import os
from pathlib import Path
from typing import List, Tuple, Dict

from tqdm import tqdm


def process_merge_file(input_file: str) -> Dict[str, List[Tuple[str, str]]]:
    """Process a single merge CSV file and return list of surviving and merged entities.

    Args:
        input_file: Path to the input CSV file

    Returns:
        Dictionary containing:
            'valid_entries': List of tuples (surviving_entity, merged_entities) for rows with Done=True
            'total_rows': Total number of data rows in the file
    """
    results = []
    total_rows = 0

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            done = row.get("Done", "").lower() == "true"
            if done:
                surviving = row["surviving_entity"].strip()
                merged = row["merged_entities"].strip()
                results.append((surviving, merged))

    return {"valid_entries": results, "total_rows": total_rows}


def process_merge_directory(input_dir: str, output_file: str):
    """Process all CSV files in a directory and create a single output file.

    Args:
        input_dir: Directory containing input CSV files
        output_file: Path where to save the output CSV file
    """
    all_results = []
    total_rows = 0
    input_path = Path(input_dir)

    # Get list of CSV files
    csv_files = list(input_path.glob("*.csv"))

    # Process all CSV files in directory with progress bar
    for file in tqdm(csv_files, desc="Processing files", unit="file"):
        file_results = process_merge_file(str(file))
        all_results.extend(file_results["valid_entries"])
        total_rows += file_results["total_rows"]

    # Write results to output file
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["surviving_entity", "merged_entities"])
        writer.writerows(all_results)

    # Calculate statistics
    valid_entries = len(all_results)
    valid_percentage = (valid_entries / total_rows * 100) if total_rows > 0 else 0

    print(f"\nProcessed {len(csv_files)} files")
    print(f"Total rows processed: {total_rows}")
    print(f"Valid merge entries found: {valid_entries} ({valid_percentage:.1f}%)")
    print(f"Results written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Process merge CSV files and combine into single output"
    )
    parser.add_argument("input_dir", help="Directory containing input CSV files")
    parser.add_argument("output_file", help="Path for output CSV file")

    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        raise ValueError(f"Input directory does not exist: {args.input_dir}")

    process_merge_directory(args.input_dir, args.output_file)


if __name__ == "__main__":
    main()
