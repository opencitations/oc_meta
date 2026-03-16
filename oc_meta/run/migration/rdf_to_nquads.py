#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2026, Arcangelo Massari <arcangelo.massari@unibo.it>
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
import multiprocessing
import os
import zipfile
from functools import partial
from pathlib import Path

import py7zr
from pebble import ProcessPool
from rdflib import Dataset
from rich.console import Console
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TextColumn,
                           TimeElapsedColumn, TimeRemainingColumn)
from rich_argparse import RichHelpFormatter

console = Console(stderr=True)


def convert_jsonld_to_nquads(jsonld_content: str) -> str:
    graph = Dataset(default_union=True)
    graph.parse(data=jsonld_content, format="json-ld")
    return graph.serialize(format="nquads")


def process_zip_file(zip_path: Path, output_dir: Path, input_dir_path: Path, compress: bool) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        json_files = [name for name in zf.namelist() if name.endswith(".json")]
        json_filename = json_files[0]
        jsonld_content = zf.read(json_filename).decode("utf-8")

    nquads_output = convert_jsonld_to_nquads(jsonld_content)

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


def find_zip_files(input_path: Path, mode: str) -> list[Path]:
    if mode == "prov":
        return list(input_path.rglob("se.zip"))
    if mode == "data":
        all_zips = input_path.rglob("*.zip")
        return [z for z in all_zips if z.name != "se.zip" and "prov" not in z.parts]
    return list(input_path.rglob("*.zip"))


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

    zip_files = find_zip_files(input_path, args.mode)
    total_files = len(zip_files)

    mode_labels = {"all": "", "data": "data ", "prov": "provenance "}
    console.print(f"Found {total_files} {mode_labels[args.mode]}ZIP files in {input_path}")
    console.print(f"Output directory: {output_path}")
    console.print(f"Workers: {num_workers}")
    console.print(f"Compression: {'7z' if args.compress else 'none'}")

    fail_count = 0
    task_func = partial(process_zip_file, output_dir=output_path, input_dir_path=input_path, compress=args.compress)

    with ProcessPool(max_workers=num_workers) as pool:
        future = pool.map(task_func, zip_files)
        iterator = future.result()

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
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
