# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice

from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import console
from oc_meta.run.infodir._common import (
    COUNTERS_PER_CHUNK,
    ENTITY_TYPES,
    CounterKey,
    DataScanResult,
    SourceDataError,
    SparseCounterStore,
    scan_data,
    scan_provenance,
    write_json,
)

DEFAULT_WORKERS = 4
DEFAULT_MAX_EXAMPLES = 100


@dataclass
class ResultCategory:
    max_examples: int
    total: int = 0
    examples: list[dict[str, object]] = field(default_factory=list)

    def add(self, example: dict[str, object]) -> None:
        self.total += 1
        if len(self.examples) < self.max_examples:
            self.examples.append(example)

    def report(self) -> dict[str, object]:
        return {
            "total": self.total,
            "examples": self.examples,
            "truncated": self.total > len(self.examples),
        }


@dataclass(frozen=True)
class CounterFiles:
    entity: dict[CounterKey, str]
    provenance: dict[CounterKey, str]


def check_info_dir(
    root_path: str,
    info_dir: str,
    output_path: str,
    workers: int = DEFAULT_WORKERS,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
    temp_dir: str | None = None,
) -> tuple[dict[str, object], int]:
    root_path = os.path.abspath(root_path)
    info_dir = os.path.abspath(info_dir)
    if max_examples < 0:
        raise ValueError("max_examples must be non-negative")

    scratch_parent = (
        os.path.abspath(temp_dir) if temp_dir is not None else os.path.dirname(info_dir)
    )
    provenance_scan = None
    data_scan = None
    try:
        with tempfile.TemporaryDirectory(
            prefix=".check-info-dir.", dir=scratch_parent
        ) as scratch_dir:
            store = SparseCounterStore(scratch_dir)
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
                report = _compare_info_dir(
                    root_path,
                    info_dir,
                    store,
                    provenance_scan.zip_files,
                    provenance_scan.entities,
                    data_scan,
                    max_examples,
                )
            finally:
                store.close()
    except SourceDataError as error:
        report = _scan_failed_report(
            root_path,
            info_dir,
            str(error),
            provenance_scan.zip_files if provenance_scan is not None else 0,
            provenance_scan.entities if provenance_scan is not None else 0,
            data_scan.zip_files if data_scan is not None else 0,
            data_scan.entities if data_scan is not None else 0,
        )
        write_json(output_path, report)
        console.print(f"Info directory check failed: {error}")
        console.print(f"Report saved to {os.path.abspath(output_path)}")
        return report, 2

    write_json(output_path, report)
    status = report["status"]
    exit_code = 0 if status == "aligned" else 1
    console.print(f"Info directory status: {status}")
    console.print(f"Report saved to {os.path.abspath(output_path)}")
    return report, exit_code


def _compare_info_dir(
    root_path: str,
    info_dir: str,
    store: SparseCounterStore,
    provenance_zip_files: int,
    provenance_entities: int,
    data_scan: DataScanResult,
    max_examples: int,
) -> dict[str, object]:
    entity_mismatches = ResultCategory(max_examples)
    provenance_mismatches = ResultCategory(max_examples)
    file_errors = ResultCategory(max_examples)
    counter_files = _counter_files(info_dir, file_errors)

    expected_entity: dict[CounterKey, int] = {}
    for key in store.keys() | set(data_scan.maxima):
        expected_entity[key] = max(
            store.maximum(key),
            data_scan.maxima[key] if key in data_scan.maxima else 0,
        )

    for key in sorted(set(expected_entity) | set(counter_files.entity)):
        expected = expected_entity[key] if key in expected_entity else 0
        path = counter_files.entity[key] if key in counter_files.entity else None
        if path is None:
            entity_mismatches.add(_entity_mismatch(key, expected, None, "missing"))
            continue
        actual = _read_entity_counter(path, file_errors)
        if actual is None:
            continue
        if key not in expected_entity:
            file_errors.add(
                {
                    "path": path,
                    "error": "unexpected_entity_counter_file",
                }
            )
        if actual != expected:
            relation = "too_low" if actual < expected else "too_high"
            entity_mismatches.add(_entity_mismatch(key, expected, actual, relation))

    for key in sorted(store.keys() | set(counter_files.provenance)):
        path = (
            counter_files.provenance[key] if key in counter_files.provenance else None
        )
        _compare_provenance_file(
            store,
            key,
            path,
            provenance_mismatches,
            file_errors,
        )

    live_without_provenance = {
        "total": data_scan.missing_provenance,
        "examples": data_scan.missing_examples,
        "truncated": data_scan.missing_provenance > len(data_scan.missing_examples),
    }
    if entity_mismatches.total or provenance_mismatches.total or file_errors.total:
        status = "mismatched"
    elif data_scan.missing_provenance:
        status = "warnings"
    else:
        status = "aligned"

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "root_path": root_path,
        "info_dir": info_dir,
        "source": {
            "data_zip_files": data_scan.zip_files,
            "provenance_zip_files": provenance_zip_files,
            "data_entities": data_scan.entities,
            "provenance_entities": provenance_entities,
        },
        "entity_counter_mismatches": entity_mismatches.report(),
        "provenance_counter_mismatches": provenance_mismatches.report(),
        "counter_file_errors": file_errors.report(),
        "live_entities_without_provenance": live_without_provenance,
    }


def _counter_files(info_dir: str, file_errors: ResultCategory) -> CounterFiles:
    entity: dict[CounterKey, str] = {}
    provenance: dict[CounterKey, str] = {}
    if not os.path.isdir(info_dir):
        file_errors.add(
            {
                "path": info_dir,
                "error": "missing_info_directory",
            }
        )
        return CounterFiles(entity=entity, provenance=provenance)

    with os.scandir(info_dir) as prefix_entries:
        for prefix_entry in prefix_entries:
            if not prefix_entry.is_dir():
                continue
            prefix = prefix_entry.name
            with os.scandir(prefix_entry.path) as file_entries:
                for file_entry in file_entries:
                    if not file_entry.is_file() or not file_entry.name.endswith(".txt"):
                        continue
                    parsed = _counter_filename(file_entry.name)
                    if parsed is None:
                        file_errors.add(
                            {
                                "path": file_entry.path,
                                "error": "unexpected_counter_file",
                            }
                        )
                        continue
                    kind, short_name = parsed
                    key = (prefix, short_name)
                    if kind == "entity":
                        entity[key] = file_entry.path
                    else:
                        provenance[key] = file_entry.path
    return CounterFiles(entity=entity, provenance=provenance)


def _counter_filename(filename: str) -> tuple[str, str] | None:
    if filename.startswith("info_file_"):
        kind = "entity"
        short_name = filename.removeprefix("info_file_").removesuffix(".txt")
    elif filename.startswith("prov_file_"):
        kind = "provenance"
        short_name = filename.removeprefix("prov_file_").removesuffix(".txt")
    else:
        return None
    if short_name not in ENTITY_TYPES:
        return None
    return kind, short_name


def _read_entity_counter(path: str, file_errors: ResultCategory) -> int | None:
    with open(path, encoding="utf-8") as input_file:
        lines = input_file.read().splitlines()
    if len(lines) != 1:
        file_errors.add(
            {
                "path": path,
                "error": "invalid_entity_counter_line_count",
                "expected": 1,
                "actual": len(lines),
            }
        )
        if not lines:
            return None
    try:
        value = int(lines[0])
    except ValueError:
        file_errors.add(
            {
                "path": path,
                "error": "invalid_entity_counter_value",
                "value": lines[0],
            }
        )
        return None
    if value < 0:
        file_errors.add(
            {
                "path": path,
                "error": "invalid_entity_counter_value",
                "value": value,
            }
        )
        return None
    return value


def _compare_provenance_file(
    store: SparseCounterStore,
    key: CounterKey,
    path: str | None,
    mismatches: ResultCategory,
    file_errors: ResultCategory,
) -> None:
    expected_lines = store.maximum(key)
    if path is None:
        if expected_lines == 0:
            return
        file_errors.add(
            {
                "prefix": key[0],
                "short_name": key[1],
                "error": "missing_provenance_counter_file",
            }
        )
        for start, expected in store.iter_chunks(key):
            for offset, value in enumerate(expected):
                if value:
                    mismatches.add(
                        _provenance_mismatch(
                            key,
                            start + offset,
                            value,
                            0,
                            "missing",
                        )
                    )
        return

    if expected_lines == 0:
        file_errors.add(
            {
                "path": path,
                "error": "unexpected_provenance_counter_file",
            }
        )

    actual_lines = 0
    with open(path, encoding="utf-8") as input_file:
        while True:
            lines = list(islice(input_file, COUNTERS_PER_CHUNK))
            if not lines:
                break
            start = actual_lines + 1
            expected = store.read_span(key, start, start + len(lines) - 1)
            for offset, line in enumerate(lines):
                resource_number = start + offset
                actual = _parse_provenance_counter(
                    line,
                    path,
                    resource_number,
                    file_errors,
                )
                if actual is None:
                    continue
                expected_value = expected[offset]
                if actual != expected_value:
                    mismatches.add(
                        _provenance_mismatch(
                            key,
                            resource_number,
                            expected_value,
                            actual,
                            _counter_relation(expected_value, actual),
                        )
                    )
            actual_lines += len(lines)

    if actual_lines < expected_lines:
        for start, expected in store.iter_chunks(key, start=actual_lines + 1):
            for offset, value in enumerate(expected):
                if value:
                    mismatches.add(
                        _provenance_mismatch(
                            key,
                            start + offset,
                            value,
                            0,
                            "missing",
                        )
                    )
    if actual_lines != expected_lines:
        file_errors.add(
            {
                "path": path,
                "error": "invalid_provenance_counter_line_count",
                "expected": expected_lines,
                "actual": actual_lines,
            }
        )


def _parse_provenance_counter(
    line: str,
    path: str,
    resource_number: int,
    file_errors: ResultCategory,
) -> int | None:
    stripped = line.strip()
    if not stripped:
        return 0
    try:
        value = int(stripped)
    except ValueError:
        file_errors.add(
            {
                "path": path,
                "line": resource_number,
                "error": "invalid_provenance_counter_value",
                "value": stripped,
            }
        )
        return None
    if value < 0:
        file_errors.add(
            {
                "path": path,
                "line": resource_number,
                "error": "invalid_provenance_counter_value",
                "value": value,
            }
        )
        return None
    return value


def _entity_mismatch(
    key: CounterKey,
    expected: int,
    actual: int | None,
    relation: str,
) -> dict[str, object]:
    return {
        "prefix": key[0],
        "short_name": key[1],
        "expected": expected,
        "actual": actual,
        "relation": relation,
    }


def _provenance_mismatch(
    key: CounterKey,
    resource_number: int,
    expected: int,
    actual: int,
    relation: str,
) -> dict[str, object]:
    return {
        "prefix": key[0],
        "short_name": key[1],
        "resource_number": resource_number,
        "expected": expected,
        "actual": actual,
        "relation": relation,
    }


def _counter_relation(expected: int, actual: int) -> str:
    if expected == 0:
        return "unexpected"
    if actual == 0:
        return "missing"
    return "too_low" if actual < expected else "too_high"


def _scan_failed_report(
    root_path: str,
    info_dir: str,
    error: str,
    provenance_zip_files: int,
    provenance_entities: int,
    data_zip_files: int,
    data_entities: int,
) -> dict[str, object]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "scan_failed",
        "root_path": root_path,
        "info_dir": info_dir,
        "source": {
            "data_zip_files": data_zip_files,
            "provenance_zip_files": provenance_zip_files,
            "data_entities": data_entities,
            "provenance_entities": provenance_entities,
            "error": error,
        },
    }


def main() -> int:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Verify filesystem counters against RDF data and provenance.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("directory", type=str, help="Path to the RDF directory to scan")
    parser.add_argument("info_dir", type=str, help="Counter directory to verify")
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="check_info_dir_report.json",
        help="Output JSON report path (default: check_info_dir_report.json)",
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
        help=f"Examples retained per category (default: {DEFAULT_MAX_EXAMPLES})",
    )
    parser.add_argument(
        "--temp-dir",
        help="Temporary storage directory (default: parent of info_dir)",
    )
    args = parser.parse_args()
    _, exit_code = check_info_dir(
        args.directory,
        args.info_dir,
        args.output,
        workers=args.workers,
        max_examples=args.max_examples,
        temp_dir=args.temp_dir,
    )
    return exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
