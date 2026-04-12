#!/usr/bin/env python

# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path

import py7zr
from rdflib import Dataset
from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import collect_zip_files


def process_zip_file(zip_path: Path, output_dir: Path, input_dir_path: Path, compress: bool) -> None:
    graph = Dataset(default_union=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        json_file = next(name for name in zf.namelist() if name.endswith(".json"))
        with zf.open(json_file) as f:
            graph.parse(f, format="json-ld")

    nquads_output = graph.serialize(format="nquads")

    relative_path = zip_path.relative_to(input_dir_path)
    output_filename = str(relative_path).replace(os.sep, "-")
    output_filename = Path(output_filename).with_suffix(".nq").name
    output_nq_path = output_dir / output_filename

    with open(output_nq_path, "w", encoding="utf-8") as f:
        f.write(nquads_output)

    if compress:
        output_7z_path = output_nq_path.with_suffix(".nq.7z")
        with py7zr.SevenZipFile(output_7z_path, "w") as archive:
            archive.write(output_nq_path, output_filename)
        output_nq_path.unlink()


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Converts JSON-LD files from ZIP archives to N-Quads format.",
        formatter_class=RichHelpFormatter
    )
    parser.add_argument("input_dir", type=str, help="Input directory containing ZIP files (recursive search)")
    parser.add_argument("output_dir", type=str, help="Output directory for the converted .nq files")
    parser.add_argument("-m", "--mode", type=str, choices=["all", "data", "prov"], default="all",
                        help="Mode: 'all' for all ZIP files (default), 'data' for entity data only, 'prov' for provenance only")
    parser.add_argument("-w", "--workers", type=int, default=None, help="Number of worker processes (defaults to CPU count)")
    parser.add_argument("-c", "--compress", action="store_true", help="Compress output files using 7z format")
    args = parser.parse_args()

    input_path = Path(args.input_dir).resolve()
    output_path = Path(args.output_dir).resolve()
    num_workers = args.workers if args.workers else multiprocessing.cpu_count()

    output_path.mkdir(parents=True, exist_ok=True)

    zip_files = collect_zip_files(
        str(input_path),
        only_data=args.mode == "data",
        only_prov=args.mode == "prov",
    )
    total_files = len(zip_files)

    mode_labels = {"all": "", "data": "data ", "prov": "provenance "}
    console.print(f"Found {total_files} {mode_labels[args.mode]}ZIP files in {input_path}")
    console.print(f"Output directory: {output_path}")
    console.print(f"Workers: {num_workers}")
    console.print(f"Compression: {'7z' if args.compress else 'none'}")

    fail_count = 0
    task_func = partial(process_zip_file, output_dir=output_path, input_dir_path=input_path, compress=args.compress)

    # Use forkserver to avoid deadlocks when forking in a multi-threaded environment
    ctx = multiprocessing.get_context('forkserver')
    with ProcessPoolExecutor(max_workers=num_workers, mp_context=ctx) as executor:
        iterator = executor.map(task_func, zip_files)

        with create_progress() as progress:
            task = progress.add_task("Converting", total=total_files)
            while True:
                try:
                    next(iterator)
                    progress.update(task, advance=1)
                except StopIteration:
                    break
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                    fail_count += 1
                    progress.update(task, advance=1)

    console.print()
    console.print("Final report")
    console.print(f"  Success: {total_files - fail_count}")
    console.print(f"  Failed:  {fail_count}")


if __name__ == "__main__":  # pragma: no cover
    main()
