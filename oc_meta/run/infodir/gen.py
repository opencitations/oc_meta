#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from datetime import datetime, timezone

from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import console
from oc_meta.run.infodir._common import (
    CounterKey,
    SourceDataError,
    SparseCounterStore,
    scan_data,
    scan_provenance,
    write_json,
)

DEFAULT_WORKERS = 4
DEFAULT_MAX_EXAMPLES = 100


def generate_info_dir(
    root_path: str,
    info_dir: str,
    workers: int = DEFAULT_WORKERS,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
    report_path: str | None = None,
) -> dict[str, object]:
    root_path = os.path.abspath(root_path)
    info_dir = os.path.abspath(info_dir)
    if os.path.exists(info_dir):
        raise FileExistsError(f"Info directory already exists: {info_dir}")
    if max_examples < 0:
        raise ValueError("max_examples must be non-negative")

    destination_parent = os.path.dirname(info_dir)
    os.makedirs(destination_parent, exist_ok=True)
    staging_dir = tempfile.mkdtemp(
        prefix=f".{os.path.basename(info_dir)}.", suffix=".tmp", dir=destination_parent
    )
    scratch_dir = os.path.join(staging_dir, ".scratch")
    os.makedirs(scratch_dir)
    store = SparseCounterStore(scratch_dir)
    published = False

    try:
        provenance_scan = scan_provenance(root_path, store, workers)
        data_scan = scan_data(
            root_path,
            store,
            workers,
            max_examples,
        )
        if provenance_scan.zip_files == 0 and data_scan.zip_files == 0:
            raise SourceDataError(f"No RDF ZIP files found in {root_path}")

        all_keys = store.keys() | set(data_scan.maxima)
        entity_maxima: dict[CounterKey, int] = {}
        for key in all_keys:
            entity_maxima[key] = max(
                data_scan.maxima[key] if key in data_scan.maxima else 0,
                store.maximum(key),
            )

        for (prefix, short_name), maximum in sorted(entity_maxima.items()):
            prefix_dir = os.path.join(staging_dir, prefix)
            os.makedirs(prefix_dir, exist_ok=True)
            with open(
                os.path.join(prefix_dir, f"info_file_{short_name}.txt"),
                "w",
                encoding="utf-8",
            ) as output_file:
                output_file.write(f"{maximum}\n")

        for key in sorted(store.keys()):
            prefix, short_name = key
            store.render(
                key,
                os.path.join(staging_dir, prefix, f"prov_file_{short_name}.txt"),
            )

        store.close()
        shutil.rmtree(scratch_dir)
        status = (
            "generated_with_warnings" if data_scan.missing_provenance else "generated"
        )
        report: dict[str, object] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "root_path": root_path,
            "info_dir": info_dir,
            "source": {
                "data_zip_files": data_scan.zip_files,
                "provenance_zip_files": provenance_scan.zip_files,
                "data_entities": data_scan.entities,
                "provenance_entities": provenance_scan.entities,
            },
            "live_entities_without_provenance": {
                "total": data_scan.missing_provenance,
                "examples": data_scan.missing_examples,
                "truncated": data_scan.missing_provenance
                > len(data_scan.missing_examples),
            },
        }

        os.replace(staging_dir, info_dir)
        published = True
    finally:
        store.close()
        if not published and os.path.isdir(staging_dir):
            shutil.rmtree(staging_dir)

    if report_path is not None:
        write_json(report_path, report)

    console.print(f"Generated info directory: {info_dir}")
    console.print(f"Live entities without provenance: {data_scan.missing_provenance}")
    if report_path is not None:
        console.print(f"Report saved to {os.path.abspath(report_path)}")
    return report


def main() -> int:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Scan RDF directories and populate filesystem counter files.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("directory", type=str, help="Path to the RDF directory to scan")
    parser.add_argument("info_dir", type=str, help="New counter directory to create")
    parser.add_argument(
        "-o",
        "--output",
        help="Generation report path (default: <info_dir>.generation-report.json)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Worker processes (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=DEFAULT_MAX_EXAMPLES,
        help=f"Examples retained per warning category (default: {DEFAULT_MAX_EXAMPLES})",
    )
    args = parser.parse_args()
    report_path = (
        args.output
        if args.output is not None
        else f"{os.path.abspath(args.info_dir)}.generation-report.json"
    )
    generate_info_dir(
        args.directory,
        args.info_dir,
        workers=args.workers,
        max_examples=args.max_examples,
        report_path=report_path,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
