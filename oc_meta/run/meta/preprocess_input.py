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
from typing import List

import redis
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich_argparse import RichHelpFormatter

from oc_meta.lib.file_manager import get_csv_data, write_csv

console = Console()


class ProcessingStats:
    def __init__(self):
        self.total_rows = 0
        self.duplicate_rows = 0
        self.existing_ids_rows = 0
        self.processed_rows = 0


def create_redis_connection(host: str, port: int, db: int = 10) -> redis.Redis:
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


def check_ids_existence(ids: str, redis_client: redis.Redis) -> bool:
    if not ids:
        return False

    id_list = ids.split()

    for id_str in id_list:
        if not redis_client.exists(id_str):
            return False

    return True


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


def process_csv_file(
    input_file: str,
    output_dir: str,
    current_file_num: int,
    redis_client: redis.Redis,
    rows_per_file: int = 3000,
    seen_rows: set | None = None,
    pending_rows: list | None = None,
) -> tuple[int, ProcessingStats, list]:
    rows_to_write = pending_rows if pending_rows is not None else []
    file_num = current_file_num
    seen_rows = seen_rows if seen_rows is not None else set()

    stats = ProcessingStats()

    data = get_csv_data(input_file, clean_data=False)

    for row in data:
        stats.total_rows += 1
        row_hash = frozenset(row.items())

        if row_hash in seen_rows:
            stats.duplicate_rows += 1
            continue

        seen_rows.add(row_hash)

        if check_ids_existence(row["id"], redis_client):
            stats.existing_ids_rows += 1
            continue

        stats.processed_rows += 1
        rows_to_write.append(row)

        if len(rows_to_write) >= rows_per_file:
            output_file = os.path.join(output_dir, "{}.csv".format(file_num))
            write_csv(output_file, rows_to_write)
            file_num += 1
            rows_to_write = []

    return file_num, stats, rows_to_write


def print_processing_report(
    all_stats: List[ProcessingStats], input_files: List[str]
) -> None:
    total_stats = ProcessingStats()
    for stats in all_stats:
        total_stats.total_rows += stats.total_rows
        total_stats.duplicate_rows += stats.duplicate_rows
        total_stats.existing_ids_rows += stats.existing_ids_rows
        total_stats.processed_rows += stats.processed_rows

    table = Table(title="Processing Report")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total input files processed", str(len(input_files)))
    table.add_row("Total input rows", str(total_stats.total_rows))
    table.add_row("Rows discarded (duplicates)", str(total_stats.duplicate_rows))
    table.add_row("Rows discarded (existing IDs)", str(total_stats.existing_ids_rows))
    table.add_row("Rows written to output", str(total_stats.processed_rows))

    if total_stats.total_rows > 0:
        duplicate_percent = (total_stats.duplicate_rows / total_stats.total_rows) * 100
        existing_percent = (
            total_stats.existing_ids_rows / total_stats.total_rows
        ) * 100
        processed_percent = (total_stats.processed_rows / total_stats.total_rows) * 100

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
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    csv_files = get_csv_files(args.input_dir)
    if not csv_files:
        console.print(
            "[red]No CSV files found in directory: {}[/red]".format(args.input_dir)
        )
        return 1

    console.print(
        "Found [green]{}[/green] CSV files to process".format(len(csv_files))
    )

    redis_client = create_redis_connection(
        args.redis_host, args.redis_port, args.redis_db
    )

    current_file_num = 0
    all_stats = []
    seen_rows: set = set()
    pending_rows: list = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Processing", total=len(csv_files))
        for csv_file in csv_files:
            progress.update(task, description=f"Processing [cyan]{os.path.basename(csv_file)}[/cyan]")
            current_file_num, stats, pending_rows = process_csv_file(
                csv_file,
                args.output_dir,
                current_file_num,
                redis_client,
                rows_per_file=args.rows_per_file,
                seen_rows=seen_rows,
                pending_rows=pending_rows,
            )
            all_stats.append(stats)
            progress.advance(task)

    if pending_rows:
        output_file = os.path.join(args.output_dir, "{}.csv".format(current_file_num))
        write_csv(output_file, pending_rows)

    print_processing_report(all_stats, csv_files)

    return 0


if __name__ == "__main__":  # pragma: no cover
    main()
