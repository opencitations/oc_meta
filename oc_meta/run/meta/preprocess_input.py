#!/usr/bin/python

# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, List

import redis
import yaml
from rich.table import Table
from rich_argparse import RichHelpFormatter

from oc_meta.constants import QLEVER_BATCH_SIZE, QLEVER_MAX_WORKERS
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import get_csv_data, normalize_path, write_csv
from oc_meta.lib.sparql import run_queries_parallel
from oc_meta.run.meta.merge_csv import resolve_output_path

DATACITE_PREFIX = "http://purl.org/spar/datacite/"


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


def check_ids_sparql(
    identifiers: set[str],
    endpoint_url: str,
    workers: int = QLEVER_MAX_WORKERS,
    progress_callback: Callable[[int], None] | None = None,
) -> set[str]:
    if not identifiers:
        return set()

    id_list = sorted(identifiers)
    batch_queries: list[str] = []
    batch_sizes: list[int] = []

    for i in range(0, len(id_list), QLEVER_BATCH_SIZE):
        batch = id_list[i : i + QLEVER_BATCH_SIZE]
        values_entries = []
        for id_str in batch:
            schema, value = id_str.split(":", 1)
            escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
            values_entries.append(
                '("{}"^^xsd:string datacite:{})'.format(escaped_value, schema)
            )

        query = (
            "PREFIX datacite: <http://purl.org/spar/datacite/>\n"
            "PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>\n"
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
            "SELECT ?val ?scheme WHERE {{\n"
            "  VALUES (?val ?scheme) {{ {} }}\n"
            "  ?id literal:hasLiteralValue ?val ;\n"
            "      datacite:usesIdentifierScheme ?scheme .\n"
            "}}"
        ).format(" ".join(values_entries))
        batch_queries.append(query)
        batch_sizes.append(len(batch))

    all_bindings = run_queries_parallel(
        endpoint_url, batch_queries, batch_sizes, workers, progress_callback
    )

    found: set[str] = set()
    for bindings in all_bindings:
        for result in bindings:
            val = result["val"]["value"]
            scheme_uri = result["scheme"]["value"]
            scheme = (
                scheme_uri[len(DATACITE_PREFIX) :]
                if scheme_uri.startswith(DATACITE_PREFIX)
                else scheme_uri
            )
            found.add("{}:{}".format(scheme, val))

    return found


def get_csv_files(directory: str) -> List[str]:
    if not os.path.isdir(directory):
        raise ValueError("The specified path '{}' is not a directory".format(directory))

    return [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".csv") and os.path.isfile(os.path.join(directory, f))
    ]


def collect_rows_from_file(file_path: str) -> FileResult:
    data = get_csv_data(file_path, clean_data=False)
    stats = ProcessingStats()
    stats.total_rows = len(data)
    valid_rows: list[tuple[tuple[tuple[str, str], ...], dict[str, str]]] = []
    for row in data:
        row_hash = tuple(sorted(row.items()))
        valid_rows.append((row_hash, row))
    return FileResult(file_path=file_path, rows=valid_rows, stats=stats)


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


def filter_sparql_results(
    results: list[FileResult],
    found_ids: set[str],
) -> None:
    for result in results:
        filtered: list[tuple[tuple[tuple[str, str], ...], dict[str, str]]] = []
        for row_hash, row in result.rows:
            ids_str = row["id"]
            if ids_str:
                row_ids = ids_str.split()
                if row_ids and all(id_str in found_ids for id_str in row_ids):
                    result.stats.existing_ids_rows += 1
                    continue
            filtered.append((row_hash, row))
        result.rows = filtered


def deduplicate_and_write(
    results: list[FileResult],
    output_path: str,
    rows_per_file: int | None = None,
    subfolders: bool = False,
) -> ProcessingStats:
    def chunk_file(num: int) -> str:
        if subfolders:
            return os.path.join(output_path, str(num), "{}.csv".format(num))
        return os.path.join(output_path, "{}.csv".format(num))

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

                if rows_per_file and len(rows_to_write) >= rows_per_file:
                    write_csv(chunk_file(file_num), rows_to_write)
                    file_num += 1
                    rows_to_write = []

            progress.advance(task)

    if rows_to_write:
        output_file = (
            chunk_file(file_num) if rows_per_file else resolve_output_path(output_path)
        )
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
        description=(
            "Split the input CSVs declared in a meta_config.yaml into chunks. "
            "Input directory and SPARQL endpoint are read from the config; the worker "
            "count is set with --workers. "
            "When rdf_files_only is True each chunk is written to its own subfolder "
            "(<output>/<n>/<n>.csv), so it can be processed and re-indexed one at a time "
            "(meta_process does not upload, so cross-chunk de-duplication needs the "
            "triplestore refreshed between chunks). When rdf_files_only is False all chunks "
            "share one folder, since meta_process uploads inline and resolves cross-chunk "
            "duplicates within a single run."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("config", help="Path to meta_config.yaml")
    parser.add_argument("output", help="Directory where chunk files are written")

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--rows-per-file",
        type=int,
        default=None,
        help="Split output into files of N rows each (default: 3000)",
    )
    output_group.add_argument(
        "--single-file",
        action="store_true",
        help="Write all output rows to a single CSV file",
    )

    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument(
        "--sparql",
        action="store_true",
        help="Drop rows whose IDs all already exist on Meta, queried via the config triplestore_url",
    )
    filter_group.add_argument(
        "--redis-port",
        type=int,
        help="Drop rows whose IDs all already exist, checked against Redis on this port",
    )
    parser.add_argument(
        "--redis-host", default="localhost", help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-db", type=int, default=10, help="Redis database number (default: 10)"
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        settings = yaml.full_load(f)

    input_dir = normalize_path(settings["input_csv_dir"])
    sparql_endpoint = settings["triplestore_url"]
    rdf_files_only = settings["rdf_files_only"]

    if args.single_file:
        rows_per_file = None
    elif args.rows_per_file is not None:
        rows_per_file = args.rows_per_file
    else:
        rows_per_file = 3000

    if rows_per_file:
        os.makedirs(args.output, exist_ok=True)

    csv_files = get_csv_files(input_dir)
    if not csv_files:
        console.print(
            "[red]No CSV files found in directory: {}[/red]".format(input_dir)
        )
        return 1

    subfolders = rows_per_file is not None and rdf_files_only
    layout = "one subfolder per chunk" if subfolders else "single folder"
    if args.redis_port is not None:
        mode = "redis"
    elif args.sparql:
        mode = "sparql"
    else:
        mode = "split-only"

    console.print(
        "Found [green]{}[/green] CSV files; [green]{}[/green] workers; mode [green]{}[/green]; layout [green]{}[/green]".format(
            len(csv_files), args.workers, mode, layout
        )
    )

    file_order = {f: i for i, f in enumerate(csv_files)}

    if args.redis_port is not None:
        results: list[FileResult] = []
        with create_progress() as progress:
            task = progress.add_task("Filtering existing IDs", total=len(csv_files))
            with ProcessPoolExecutor(
                max_workers=args.workers,
                mp_context=multiprocessing.get_context("forkserver"),
            ) as executor:
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
                    results.append(future.result())
                    progress.advance(task)
    else:
        results = []
        with create_progress() as progress:
            task = progress.add_task("Reading CSV files", total=len(csv_files))
            with ProcessPoolExecutor(
                max_workers=args.workers,
                mp_context=multiprocessing.get_context("forkserver"),
            ) as executor:
                futures = {
                    executor.submit(collect_rows_from_file, f): f for f in csv_files
                }
                for future in as_completed(futures):
                    results.append(future.result())
                    progress.advance(task)

    results.sort(key=lambda r: file_order[r.file_path])

    if args.sparql:
        all_ids: set[str] = set()
        for result in results:
            for _hash, row in result.rows:
                ids_str = row["id"]
                if ids_str:
                    all_ids.update(ids_str.split())

        if all_ids:
            console.print(
                "Checking [green]{}[/green] unique identifiers against SPARQL endpoint".format(
                    len(all_ids)
                )
            )
            with create_progress() as progress:
                task = progress.add_task("Querying SPARQL", total=len(all_ids))

                def on_batch(batch_size: int) -> None:
                    progress.advance(task, batch_size)

                found_ids = check_ids_sparql(
                    all_ids, sparql_endpoint, args.workers, on_batch
                )
        else:
            found_ids = set()

        filter_sparql_results(results, found_ids)

    total_stats = deduplicate_and_write(
        results, args.output, rows_per_file, subfolders=subfolders
    )

    print_processing_report(total_stats, len(csv_files))

    return 0


if __name__ == "__main__":  # pragma: no cover
    main()
