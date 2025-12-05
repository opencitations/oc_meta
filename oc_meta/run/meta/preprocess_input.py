#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2025 Arcangelo <arcangelo.massari@unibo.it>
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
from typing import List, Union

import redis
from sparqlite import SPARQLClient
from tqdm import tqdm


class ProcessingStats(object):
    """Class to track processing statistics"""
    def __init__(self):
        self.total_rows = 0
        self.duplicate_rows = 0
        self.existing_ids_rows = 0
        self.processed_rows = 0

def create_redis_connection(host: str, port: int, db: int = 10) -> redis.Redis:
    """Create and return a Redis connection."""
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        decode_responses=True
    )

def check_ids_existence_redis(ids: str, redis_client: redis.Redis) -> bool:
    """
    Check if all IDs in the input string exist in Redis.
    Returns True if all IDs exist, False otherwise.
    """
    if not ids:
        return False

    id_list = ids.split()
    
    for id_str in id_list:
        if not redis_client.get(id_str):
            return False
    
    return True

def check_ids_existence_sparql(ids: str, sparql_endpoint: str) -> bool:
    """
    Check if all IDs in the input string exist in the SPARQL endpoint.
    Returns True if all IDs exist, False otherwise.
    """
    if not ids:
        return False

    id_list = ids.split()

    with SPARQLClient(sparql_endpoint, max_retries=5, backoff_factor=5) as client:
        for id_str in id_list:
            escaped_id = id_str.replace("'", "\\'").replace('"', '\\"')

            parts = escaped_id.split(":", 1)
            scheme = parts[0]
            value = parts[1]

            query = f"""
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>

            ASK {{
                ?identifier datacite:usesIdentifierScheme datacite:{scheme} ;
                            literal:hasLiteralValue ?value .
                FILTER(
                    ?value = "{value}" ||
                    ?value = "{value}"^^xsd:string
                )
            }}
            """

            try:
                results = client.query(query)
                if not results.get('boolean', False):
                    return False
            except Exception:
                return False

    return True

def check_ids_existence(ids: str, storage_type: str, storage_reference: Union[redis.Redis, str, None]) -> bool:
    """
    Check if all IDs in the input string exist in the storage.

    Args:
        ids: String of space-separated IDs to check
        storage_type: Either 'redis', 'sparql', or None to skip checking
        storage_reference: Redis client, SPARQL endpoint URL, or None

    Returns:
        True if all IDs exist, False otherwise, or False if storage_type is None
    """
    if storage_type is None:
        return False
    elif storage_type == 'redis':
        return check_ids_existence_redis(ids, storage_reference)
    elif storage_type == 'sparql':
        return check_ids_existence_sparql(ids, storage_reference)
    else:
        raise ValueError(f"Invalid storage type: {storage_type}. Must be 'redis', 'sparql', or None")

def get_csv_files(directory: str) -> List[str]:
    """Get all CSV files in the specified directory (first level only)."""
    if not os.path.isdir(directory):
        raise ValueError("The specified path '{}' is not a directory".format(directory))
    
    return [
        os.path.join(directory, f) 
        for f in os.listdir(directory) 
        if f.endswith('.csv') and os.path.isfile(os.path.join(directory, f))
    ]

def process_csv_file(input_file, output_dir, current_file_num, rows_per_file=3000,
                     storage_type='redis', storage_reference=None, redis_db=10,
                     redis_host='localhost', redis_port=6379, seen_rows=None, pending_rows=None):
    """
    Process a single CSV file and write non-duplicate rows with non-existing IDs to output files.

    Args:
        input_file: Path to the input CSV file
        output_dir: Directory where output files will be written
        current_file_num: Number to use for the next output file
        rows_per_file: Number of rows per output file
        storage_type: Type of storage to check IDs against ('redis', 'sparql', or None to skip)
        storage_reference: Redis client or SPARQL endpoint URL. If None and storage_type is 'redis',
                          a new connection will be created
        redis_db: Redis database number to use if storage_type is 'redis' and storage_reference is None
        redis_host: Redis host if storage_type is 'redis' and storage_reference is None
        redis_port: Redis port if storage_type is 'redis' and storage_reference is None
        seen_rows: Set of previously seen rows (for cross-file deduplication)
        pending_rows: List of rows waiting to be written (for cross-file batching)

    Returns:
        Tuple of (next file number, processing statistics, pending rows)
    """
    rows_to_write = pending_rows if pending_rows is not None else []
    file_num = current_file_num
    seen_rows = seen_rows if seen_rows is not None else set()
    
    if storage_type == 'redis':
        storage_ref = storage_reference if storage_reference is not None else create_redis_connection(redis_host, redis_port, redis_db)
    else:
        storage_ref = storage_reference
        
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
                    
                    if check_ids_existence(row['id'], storage_type, storage_ref):
                        stats.existing_ids_rows += 1
                        continue
                        
                    stats.processed_rows += 1
                    rows_to_write.append(row)

                    if len(rows_to_write) >= rows_per_file:
                        output_file = os.path.join(output_dir, "{}.csv".format(file_num))
                        with open(output_file, 'w', encoding='utf-8', newline='') as out_f:
                            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(rows_to_write)
                        file_num += 1
                        rows_to_write = []
            break
            
        except csv.Error as e:
            if "field larger than field limit" in str(e):
                csv.field_size_limit(int(csv.field_size_limit() * 2))
            else:
                raise e

    return file_num, stats, rows_to_write

def print_processing_report(all_stats: List[ProcessingStats], input_files: List[str], storage_type: str) -> None:
    """Print a detailed report of the processing statistics."""
    total_stats = ProcessingStats()
    for stats in all_stats:
        total_stats.total_rows += stats.total_rows
        total_stats.duplicate_rows += stats.duplicate_rows
        total_stats.existing_ids_rows += stats.existing_ids_rows
        total_stats.processed_rows += stats.processed_rows

    print("\nProcessing Report:")
    print("=" * 50)
    if storage_type:
        print("Storage type used: {}".format(storage_type.upper()))
    else:
        print("Storage type used: None (ID checking skipped)")
    print("Total input files processed: {}".format(len(input_files)))
    print("Total input rows: {}".format(total_stats.total_rows))
    print("Rows discarded (duplicates): {}".format(total_stats.duplicate_rows))
    if storage_type:
        print("Rows discarded (existing IDs): {}".format(total_stats.existing_ids_rows))
    print("Rows written to output: {}".format(total_stats.processed_rows))
    
    if total_stats.total_rows > 0:
        duplicate_percent = (total_stats.duplicate_rows / total_stats.total_rows) * 100
        processed_percent = (total_stats.processed_rows / total_stats.total_rows) * 100

        print("\nPercentages:")
        print("Duplicate rows: {:.1f}%".format(duplicate_percent))
        if storage_type:
            existing_percent = (total_stats.existing_ids_rows / total_stats.total_rows) * 100
            print("Existing IDs: {:.1f}%".format(existing_percent))
        print("Processed rows: {:.1f}%".format(processed_percent))

def main():
    parser = argparse.ArgumentParser(
        description="Process CSV files and optionally check IDs against a storage system (Redis or SPARQL)"
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
        "--rows-per-file",
        type=int,
        default=3000,
        help="Number of rows per output file (default: 3000)"
    )
    parser.add_argument(
        "--storage-type",
        choices=["redis", "sparql"],
        help="Storage type to check IDs against (redis or sparql). If not specified, ID checking is skipped"
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=10,
        help="Redis database number to use if storage type is redis (default: 10)"
    )
    parser.add_argument(
        "--sparql-endpoint",
        help="SPARQL endpoint URL if storage type is sparql"
    )
    args = parser.parse_args()

    if args.storage_type == "sparql" and not args.sparql_endpoint:
        print("Error: --sparql-endpoint is required when --storage-type is sparql")
        return 1

    os.makedirs(args.output_dir, exist_ok=True)

    try:
        csv_files = get_csv_files(args.input_dir)
        if not csv_files:
            print("No CSV files found in directory: {}".format(args.input_dir))
            return 1

        print("Found {} CSV files to process".format(len(csv_files)))

        storage_reference = None
        storage_type = args.storage_type

        if storage_type:
            print("Using {} for ID existence checking".format(storage_type.upper()))
            if storage_type == "redis":
                storage_reference = create_redis_connection(args.redis_host, args.redis_port, args.redis_db)
            else:
                storage_reference = args.sparql_endpoint
        else:
            print("Skipping ID existence checking")
        
        current_file_num = 0
        all_stats = []
        seen_rows = set()
        pending_rows = []
        
        for csv_file in tqdm(csv_files, desc="Processing CSV files"):
            current_file_num, stats, pending_rows = process_csv_file(
                csv_file,
                args.output_dir,
                current_file_num,
                rows_per_file=args.rows_per_file,
                storage_type=storage_type,
                storage_reference=storage_reference,
                redis_db=args.redis_db,
                redis_host=args.redis_host,
                redis_port=args.redis_port,
                seen_rows=seen_rows,
                pending_rows=pending_rows
            )
            all_stats.append(stats)
        
        if pending_rows:
            output_file = os.path.join(args.output_dir, "{}.csv".format(current_file_num))
            with open(output_file, 'w', encoding='utf-8', newline='') as out_f:
                writer = csv.DictWriter(out_f, fieldnames=pending_rows[0].keys())
                writer.writeheader()
                writer.writerows(pending_rows)
            
        print_processing_report(all_stats, csv_files, storage_type)
            
    except Exception as e:
        print("Error: {}".format(str(e)))
        return 1

    return 0

if __name__ == "__main__":
    main() 