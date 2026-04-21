#!/usr/bin/env python

# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import sys
import zipfile
from pathlib import Path

from rdflib import Dataset
from rich_argparse import RichHelpFormatter

from oc_meta.lib.file_manager import collect_zip_files


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


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Streams N-Quads from JSON-LD ZIP archives to stdout.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("rdf_dir", type=str, help="Root directory containing RDF ZIP archives")
    parser.add_argument("-m", "--mode", type=str, choices=["all", "data", "prov"], default="all",
                        help="Mode: 'all' for all ZIP files (default), 'data' for entity data only, 'prov' for provenance only")
    parser.add_argument("-w", "--workers", type=int, default=None, help="Number of worker processes (defaults to min(8, CPU count))")
    args = parser.parse_args()

    rdf_path = Path(args.rdf_dir).resolve()
    num_workers = args.workers if args.workers else min(8, multiprocessing.cpu_count())

    zip_files = collect_zip_files(
        str(rdf_path),
        only_data=args.mode == "data",
        only_prov=args.mode == "prov",
    )

    stdout = sys.stdout.buffer
    ctx = multiprocessing.get_context("forkserver")
    with ctx.Pool(processes=num_workers) as pool:
        for result in pool.imap_unordered(convert_zip_to_nquads, zip_files, chunksize=10):
            stdout.write(result)
            stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    main()
