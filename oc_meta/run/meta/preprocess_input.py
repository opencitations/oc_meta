#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Arcangelo <arcangelo.massari@unibo.it>
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
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List

import redis
from rich.table import Table
from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import get_csv_data, write_csv


@dataclass
class ProcessingStats:
    total_rows: int = 0
    duplicate_rows: int = 0
    existing_ids_rows: int = 0
    processed_rows: int = 0


@dataclass
class FileResult:
    file_path: str
    rows: list[tuple[tuple[tuple[str, str], ...], dict[str, str]]]
    stats: ProcessingStats


def create_redis_connection(host: str, port: int, db: int = 10) -> redis.Redis:
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


def check_ids_existence_batch(
    rows: list[dict[str, str]], redis_client: redis.Redis
) -> list[bool]:
    row_id_lists: list[list[str]] = []
    for row in rows:
        ids_str = row["id"]
        row_id_lists.append(ids_str.split() if ids_str else [])

    pipe = redis_client.pipeline()
    for id_list in row_id_lists:
        for id_str in id_list:
            pipe.exists(id_str)

    results = pipe.execute()

    row_results: list[bool] = []
    idx = 0
    for id_list in row_id_lists:
        if not id_list:
            row_results.append(False)
        else:
            all_exist = True
            for _ in id_list:
                if not results[idx]:
                    all_exist = False
                idx += 1
            row_results.append(all_exist)

    return row_results


def get_csv_files(directory: str) -> List[str]:
    if not os.path.isdir(directory):
        raise ValueError(
            "The specified path '{}' is not a directory".format(directory)
        )

    return [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".csv") and os.path.isfile(os.path.join(directory, f))
    ]


def filter_existing_ids_from_file(
    file_path: str, redis_host: str, redis_port: int, redis_db: int
) -> FileResult:
    redis_client = create_redis_connection(redis_host, redis_port, redis_db)
    data = get_csv_data(file_path, clean_data=False)

    stats = ProcessingStats()
    stats.total_rows = len(data)

    existence_results = check_ids_existence_batch(data, redis_client)

    valid_rows: list[tuple[tuple[tuple[str, str], ...], dict[str, str]]] = []
    for row, exists in zip(data, existence_results):
        if exists:
            stats.existing_ids_rows += 1
        else:
            row_hash = tuple(sorted(row.items()))
            valid_rows.append((row_hash, row))

    return FileResult(file_path=file_path, rows=valid_rows, stats=stats)


def deduplicate_and_write(
    results: list[FileResult], output_dir: str, rows_per_file: int
) -> ProcessingStats:
    seen_rows: set[tuple[tuple[str, str], ...]] = set()
    rows_to_write: list[dict[str, str]] = []
    file_num = 0

    total_stats = ProcessingStats()

    with create_progress() as progress:
        task = progress.add_task("Deduplicating and writing", total=len(results))

        for result in results:
            total_stats.total_rows += result.stats.total_rows
            total_stats.existing_ids_rows += result.stats.existing_ids_rows

            for row_hash, row in result.rows:
                if row_hash in seen_rows:
                    total_stats.duplicate_rows += 1
                    continue

                seen_rows.add(row_hash)
                total_stats.processed_rows += 1
                rows_to_write.append(row)

                if len(rows_to_write) >= rows_per_file:
                    output_file = os.path.join(output_dir, "{}.csv".format(file_num))
                    write_csv(output_file, rows_to_write)
                    file_num += 1
                    rows_to_write = []

            progress.advance(task)

    if rows_to_write:
        output_file = os.path.join(output_dir, "{}.csv".format(file_num))
        write_csv(output_file, rows_to_write)

    return total_stats


def print_processing_report(stats: ProcessingStats, num_files: int) -> None:
    table = Table(title="Processing Report")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total input files processed", str(num_files))
    table.add_row("Total input rows", str(stats.total_rows))
    table.add_row("Rows discarded (duplicates)", str(stats.duplicate_rows))
    table.add_row("Rows discarded (existing IDs)", str(stats.existing_ids_rows))
    table.add_row("Rows written to output", str(stats.processed_rows))

    if stats.total_rows > 0:
        duplicate_percent = (stats.duplicate_rows / stats.total_rows) * 100
        existing_percent = (stats.existing_ids_rows / stats.total_rows) * 100
        processed_percent = (stats.processed_rows / stats.total_rows) * 100

        table.add_row("", "")
        table.add_row("Duplicate rows %", "{:.1f}%".format(duplicate_percent))
        table.add_row("Existing IDs %", "{:.1f}%".format(existing_percent))
        table.add_row("Processed rows %", "{:.1f}%".format(processed_percent))

    console.print(table)


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Process CSV files and check IDs against Redis",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("input_dir", help="Directory containing input CSV files")
    parser.add_argument("output_dir", help="Directory for output CSV files")
    parser.add_argument("--redis-port", type=int, required=True, help="Redis port")
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=3000,
        help="Number of rows per output file (default: 3000)",
    )
    parser.add_argument(
        "--redis-host", default="localhost", help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=10,
        help="Redis database number (default: 10)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    csv_files = get_csv_files(args.input_dir)
    if not csv_files:
        console.print(
            "[red]No CSV files found in directory: {}[/red]".format(args.input_dir)
        )
        return 1

    console.print(
        "Found [green]{}[/green] CSV files to process with [green]{}[/green] workers".format(
            len(csv_files), args.workers
        )
    )

    results: list[FileResult] = []
    file_order = {f: i for i, f in enumerate(csv_files)}

    with create_progress() as progress:
        task = progress.add_task("Filtering existing IDs", total=len(csv_files))

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    filter_existing_ids_from_file,
                    csv_file,
                    args.redis_host,
                    args.redis_port,
                    args.redis_db,
                ): csv_file
                for csv_file in csv_files
            }

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                progress.advance(task)

    results.sort(key=lambda r: file_order[r.file_path])

    total_stats = deduplicate_and_write(results, args.output_dir, args.rows_per_file)

    print_processing_report(total_stats, len(csv_files))

    return 0


if __name__ == "__main__":  # pragma: no cover
    main()
