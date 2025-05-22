#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2024 Arcangelo <arcangelo.massari@unibo.it>
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

from __future__ import annotations

import argparse
import csv
import os
from typing import List

import redis
from tqdm import tqdm

ROWS_PER_FILE = 3000
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

class ProcessingStats(object):
    """Class to track processing statistics"""
    def __init__(self):
        self.total_rows = 0
        self.duplicate_rows = 0
        self.existing_ids_rows = 0
        self.processed_rows = 0

def create_redis_connection(db: int = 10) -> redis.Redis:
    """Create and return a Redis connection."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=db,
        decode_responses=True
    )

def check_ids_existence(ids: str, redis_client: redis.Redis) -> bool:
    """
    Check if all IDs in the input string exist in Redis.
    Returns True if all IDs exist, False otherwise.
    """
    if not ids:
        return False

    id_list = ids.split()
    
    # Check each ID in Redis
    for id_str in id_list:
        if not redis_client.get(id_str):
            return False
    
    return True

def get_csv_files(directory: str) -> List[str]:
    """Get all CSV files in the specified directory (first level only)."""
    if not os.path.isdir(directory):
        raise ValueError("The specified path '{}' is not a directory".format(directory))
    
    return [
        os.path.join(directory, f) 
        for f in os.listdir(directory) 
        if f.endswith('.csv') and os.path.isfile(os.path.join(directory, f))
    ]

def process_csv_file(input_file, output_dir, current_file_num, redis_db=10, seen_rows=None, pending_rows=None):
    """
    Process a single CSV file and write non-duplicate rows with non-existing IDs to output files.
    
    Args:
        input_file: Path to the input CSV file
        output_dir: Directory where output files will be written
        current_file_num: Number to use for the next output file
        redis_db: Redis database number to use (default: 10)
        seen_rows: Set of previously seen rows (for cross-file deduplication)
        pending_rows: List of rows waiting to be written (for cross-file batching)
    
    Returns:
        Tuple of (next file number, processing statistics, pending rows)
    """
    rows_to_write = pending_rows if pending_rows is not None else []
    file_num = current_file_num
    seen_rows = seen_rows if seen_rows is not None else set()
    redis_client = create_redis_connection(redis_db)
    stats = ProcessingStats()

    while True:
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                
                for row in reader:
                    stats.total_rows += 1
                    row_hash = frozenset(row.items())

                    if row_hash in seen_rows:
                        stats.duplicate_rows += 1
                        continue
                        
                    seen_rows.add(row_hash)
                    
                    # Skip row if all IDs exist in Redis
                    if check_ids_existence(row['id'], redis_client):
                        stats.existing_ids_rows += 1
                        continue
                        
                    stats.processed_rows += 1
                    rows_to_write.append(row)
                    
                    # Write file when we reach ROWS_PER_FILE
                    if len(rows_to_write) >= ROWS_PER_FILE:
                        output_file = os.path.join(output_dir, "{}.csv".format(file_num))
                        with open(output_file, 'w', encoding='utf-8', newline='') as out_f:
                            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(rows_to_write)
                        file_num += 1
                        rows_to_write = []
            break  # Se arriviamo qui, la lettura è andata a buon fine
            
        except csv.Error as e:
            if "field larger than field limit" in str(e):
                # Aumenta il limite solo se necessario
                csv.field_size_limit(int(csv.field_size_limit() * 2))
                # Il ciclo while riproverà con il nuovo limite
            else:
                raise e

    return file_num, stats, rows_to_write

def print_processing_report(all_stats: List[ProcessingStats], input_files: List[str]) -> None:
    """Print a detailed report of the processing statistics."""
    total_stats = ProcessingStats()
    for stats in all_stats:
        total_stats.total_rows += stats.total_rows
        total_stats.duplicate_rows += stats.duplicate_rows
        total_stats.existing_ids_rows += stats.existing_ids_rows
        total_stats.processed_rows += stats.processed_rows

    print("\nProcessing Report:")
    print("=" * 50)
    print("Total input files processed: {}".format(len(input_files)))
    print("Total input rows: {}".format(total_stats.total_rows))
    print("Rows discarded (duplicates): {}".format(total_stats.duplicate_rows))
    print("Rows discarded (existing IDs): {}".format(total_stats.existing_ids_rows))
    print("Rows written to output: {}".format(total_stats.processed_rows))
    
    if total_stats.total_rows > 0:
        duplicate_percent = (total_stats.duplicate_rows / total_stats.total_rows) * 100
        existing_percent = (total_stats.existing_ids_rows / total_stats.total_rows) * 100
        processed_percent = (total_stats.processed_rows / total_stats.total_rows) * 100
        
        print("\nPercentages:")
        print("Duplicate rows: {:.1f}%".format(duplicate_percent))
        print("Existing IDs: {:.1f}%".format(existing_percent))
        print("Processed rows: {:.1f}%".format(processed_percent))

def main():
    parser = argparse.ArgumentParser(
        description="Process CSV files and check IDs against Redis"
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing input CSV files"
    )
    parser.add_argument(
        "output_dir", 
        help="Directory for output CSV files"
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=10,
        help="Redis database number to use (default: 10)"
    )
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    try:
        csv_files = get_csv_files(args.input_dir)
        if not csv_files:
            print("No CSV files found in directory: {}".format(args.input_dir))
            return 1

        print("Found {} CSV files to process".format(len(csv_files)))
        
        current_file_num = 0
        all_stats = []
        seen_rows = set()  # Set to track duplicates across all files
        pending_rows = []  # List to track rows waiting to be written
        
        for csv_file in tqdm(csv_files, desc="Processing CSV files"):
            current_file_num, stats, pending_rows = process_csv_file(
                csv_file, 
                args.output_dir, 
                current_file_num,
                args.redis_db,
                seen_rows,
                pending_rows
            )
            all_stats.append(stats)
        
        # Write any remaining rows after processing all files
        if pending_rows:
            output_file = os.path.join(args.output_dir, "{}.csv".format(current_file_num))
            with open(output_file, 'w', encoding='utf-8', newline='') as out_f:
                writer = csv.DictWriter(out_f, fieldnames=pending_rows[0].keys())
                writer.writeheader()
                writer.writerows(pending_rows)
            
        print_processing_report(all_stats, csv_files)
            
    except Exception as e:
        print("Error: {}".format(str(e)))
        return 1

    return 0

if __name__ == "__main__":
    main() 