#!/usr/bin/env python

# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import gzip
import multiprocessing
import sys
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import IO

from rdflib import Dataset
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich_argparse import RichHelpFormatter

from oc_meta.lib.file_manager import collect_zip_files

DEFAULT_LINES_PER_FILE = 10000000


def convert_zip_to_nquads(zip_path: str) -> bytes:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            json_file = next(n for n in zf.namelist() if n.endswith(".json"))
            graph = Dataset(default_union=True)
            with zf.open(json_file) as f:
                graph.parse(f, format="json-ld")
        return graph.serialize(format="nquads").encode("utf-8")
    except Exception:
        print(f"Failed to convert: {zip_path}", file=sys.stderr, flush=True)
        raise


def open_output_file(
    output_dir: Path, prefix: str, file_index: int, compress: bool
) -> gzip.GzipFile | IO[bytes]:
    extension = "nq.gz" if compress else "nq"
    output_path = output_dir / f"{prefix}.{file_index:06d}.{extension}"
    if compress:
        return gzip.open(output_path, "wb")
    return output_path.open("wb")


def write_nquads_chunks(
    results: Iterable[bytes],
    output_dir: Path,
    prefix: str,
    lines_per_file: int,
    compress: bool,
    progress: Progress | None = None,
    task_id: TaskID | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_index = 0
    line_count = 0
    output_file: gzip.GzipFile | IO[bytes] | None = None

    for result in results:
        for line in result.splitlines(keepends=True):
            if output_file is None or line_count == lines_per_file:
                if output_file is not None:
                    output_file.close()
                output_file = open_output_file(output_dir, prefix, file_index, compress)
                file_index += 1
                line_count = 0
            output_file.write(line)
            line_count += 1
        if progress is not None and task_id is not None:
            progress.advance(task_id)

    if output_file is not None:
        output_file.close()


def write_nquads_stdout(results: Iterable[bytes]) -> None:
    stdout = sys.stdout.buffer
    for result in results:
        stdout.write(result)
        stdout.flush()


def create_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=Console(stderr=True),
    )


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Streams or writes chunked N-Quads from JSON-LD ZIP archives.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "rdf_dir", type=str, help="Root directory containing RDF ZIP archives"
    )
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        choices=["all", "data", "prov"],
        default="all",
        help="Mode: 'all' for all ZIP files (default), 'data' for entity data only, 'prov' for provenance only",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (defaults to min(8, CPU count))",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=None,
        help="Directory where chunked N-Quads files are written instead of stdout",
    )
    parser.add_argument(
        "--lines-per-file",
        type=int,
        default=DEFAULT_LINES_PER_FILE,
        help="Number of N-Quads lines per output file when --output-dir is used",
    )
    parser.add_argument(
        "--gzip",
        action="store_true",
        help="Compress chunked output files with gzip when --output-dir is used",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="output",
        help="Output file prefix when --output-dir is used",
    )
    args = parser.parse_args()

    rdf_path = Path(args.rdf_dir).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None
    num_workers = args.workers if args.workers else min(8, multiprocessing.cpu_count())

    zip_files = collect_zip_files(
        str(rdf_path),
        only_data=args.mode == "data",
        only_prov=args.mode == "prov",
    )

    ctx = multiprocessing.get_context("forkserver")
    with ctx.Pool(processes=num_workers) as pool:
        results = pool.imap_unordered(convert_zip_to_nquads, zip_files, chunksize=10)
        if output_dir:
            with create_progress() as progress:
                task_id = progress.add_task(
                    "Writing N-Quads files", total=len(zip_files)
                )
                write_nquads_chunks(
                    results,
                    output_dir,
                    args.prefix,
                    args.lines_per_file,
                    args.gzip,
                    progress,
                    task_id,
                )
        else:
            write_nquads_stdout(results)


if __name__ == "__main__":  # pragma: no cover
    main()
