#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import os
from typing import List

from rich.table import Table
from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import get_csv_data, write_csv


def get_csv_files(directory: str) -> List[str]:
    if not os.path.isdir(directory):
        raise ValueError(
            "The specified path '{}' is not a directory".format(directory)
        )
    return [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".csv")
    ]


def resolve_output_path(output: str) -> str:
    if output.endswith(".csv"):
        parent_dir = os.path.dirname(output)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        return output
    os.makedirs(output, exist_ok=True)
    return os.path.join(output, "merged.csv")


def merge_csv_files(
    input_dir: str, output_path: str
) -> tuple[int, int, List[str]]:
    csv_files = get_csv_files(input_dir)
    if not csv_files:
        return 0, 0, []

    all_rows: List[dict[str, str]] = []
    fieldnames: List[str] = []

    with create_progress() as progress:
        task = progress.add_task("Merging CSV files", total=len(csv_files))

        for csv_file in csv_files:
            data = get_csv_data(csv_file, clean_data=False)
            if data:
                if not fieldnames:
                    fieldnames = list(data[0].keys())
                all_rows.extend(data)
            progress.advance(task)

    if all_rows:
        write_csv(output_path, all_rows, fieldnames=fieldnames)

    return len(all_rows), len(csv_files), fieldnames


def print_merge_report(
    total_rows: int, files_processed: int, output_path: str
) -> None:
    table = Table(title="Merge report")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Files merged", str(files_processed))
    table.add_row("Total rows", str(total_rows))
    table.add_row("Output file", output_path)

    console.print(table)


def main() -> int:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Merge multiple CSV files into a single file",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("input_dir", help="Directory containing input CSV files")
    parser.add_argument(
        "output",
        help="Output path: if ends with .csv, creates that file; otherwise, creates merged.csv in that folder",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        console.print(
            "[red]Input directory does not exist: {}[/red]".format(args.input_dir)
        )
        return 1

    output_path = resolve_output_path(args.output)

    csv_files = get_csv_files(args.input_dir)
    if not csv_files:
        console.print(
            "[red]No CSV files found in directory: {}[/red]".format(args.input_dir)
        )
        return 1

    console.print(
        "Found [green]{}[/green] CSV files to merge".format(len(csv_files))
    )

    total_rows, files_processed, _ = merge_csv_files(args.input_dir, output_path)

    print_merge_report(total_rows, files_processed, output_path)

    return 0


if __name__ == "__main__":  # pragma: no cover
    main()
