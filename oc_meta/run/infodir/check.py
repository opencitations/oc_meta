# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

import orjson
from oc_ocdm.support import get_prefix, get_resource_number, get_short_name
from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import advance_progress, console, create_progress
from oc_meta.lib.file_manager import collect_files, collect_zip_files

_worker_prov_counters: dict[str, dict[str, list[int]]] = {}


def load_counters(info_dir: str) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, list[int]]]]:
    entity_counters: dict[str, dict[str, int]] = {}
    prov_counters: dict[str, dict[str, list[int]]] = {}
    counter_files = collect_files(info_dir, "*.txt")
    with create_progress() as progress:
        task_id = progress.add_task("Loading counter files", total=len(counter_files))
        for file_path in counter_files:
            filename = os.path.basename(file_path)
            prefix = os.path.basename(os.path.dirname(file_path))
            if filename.startswith("prov_file_"):
                short_name = filename.removeprefix("prov_file_").removesuffix(".txt")
                lines: list[int] = []
                with open(file_path, "r") as f:
                    for line in f:
                        stripped = line.strip()
                        lines.append(int(stripped) if stripped else 0)
                prov_counters.setdefault(prefix, {})[short_name] = lines
            elif filename.startswith("info_file_"):
                short_name = filename.removeprefix("info_file_").removesuffix(".txt")
                with open(file_path, "r") as f:
                    first_line = f.readline().strip()
                entity_counters.setdefault(prefix, {})[short_name] = int(first_line) if first_line else 0
            advance_progress(progress, task_id)
    return entity_counters, prov_counters


def lookup_prov_counter(prefix: str, short_name: str, resource_number: int) -> int:
    prefix_counters = _worker_prov_counters.get(prefix)
    if prefix_counters is None:
        return 0
    lines = prefix_counters.get(short_name)
    if lines is None:
        return 0
    idx = resource_number - 1
    if idx < len(lines):
        return lines[idx]
    return 0


def process_zip_file(zip_file: str) -> dict:
    entities: dict[str, int] = {}
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        first_file = zip_ref.namelist()[0]
        with zip_ref.open(first_file) as entity_file:
            json_data = orjson.loads(entity_file.read())
            for graph in json_data:
                for entity in graph["@graph"]:
                    prov_entity_uri = entity["@id"]
                    parts = prov_entity_uri.split("/prov/se/")
                    entity_uri = parts[0]
                    snapshot_number = int(parts[1])
                    if entity_uri not in entities or snapshot_number > entities[entity_uri]:
                        entities[entity_uri] = snapshot_number

    mismatched_prov: list[dict] = []
    max_resource_numbers: dict[str, dict[str, int]] = {}

    for entity_uri, max_snapshot in entities.items():
        prefix = get_prefix(entity_uri)
        short_name = get_short_name(entity_uri)
        resource_number = get_resource_number(entity_uri)

        if prefix not in max_resource_numbers:
            max_resource_numbers[prefix] = {}
        if short_name not in max_resource_numbers[prefix] or resource_number > max_resource_numbers[prefix][short_name]:
            max_resource_numbers[prefix][short_name] = resource_number

        prov_counter = lookup_prov_counter(prefix, short_name, resource_number)
        if prov_counter != max_snapshot:
            mismatched_prov.append({
                "entity_uri": entity_uri,
                "expected": max_snapshot,
                "actual": prov_counter,
                "zip_file": zip_file,
            })

    return {
        "mismatched_prov_counters": mismatched_prov,
        "max_resource_numbers": max_resource_numbers,
    }


def explore_provenance_files(root_path: str, info_dir: str, output_path: str) -> None:
    entity_counters, prov_counters = load_counters(info_dir)

    with console.status("Collecting provenance zip files..."):
        prov_zip_files = collect_zip_files(root_path, only_prov=True)

    console.print(f"Found {len(prov_zip_files)} provenance zip files")
    console.print(f"Loaded counters for {len(prov_counters)} supplier prefixes")

    all_mismatched_prov: list[dict] = []
    global_max_resource: dict[str, dict[str, int]] = {}

    global _worker_prov_counters
    _worker_prov_counters = prov_counters

    chunk_size = 10000
    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("fork")) as executor:
        with create_progress() as progress:
            task_id = progress.add_task("Checking provenance entities", total=len(prov_zip_files))
            for i in range(0, len(prov_zip_files), chunk_size):
                chunk = prov_zip_files[i:i + chunk_size]
                futures = {executor.submit(process_zip_file, f): f for f in chunk}
                for future in as_completed(futures):
                    result = future.result()
                    all_mismatched_prov.extend(result["mismatched_prov_counters"])
                    for prefix, by_short in result["max_resource_numbers"].items():
                        if prefix not in global_max_resource:
                            global_max_resource[prefix] = {}
                        for short_name, resource_number in by_short.items():
                            if short_name not in global_max_resource[prefix] or resource_number > global_max_resource[prefix][short_name]:
                                global_max_resource[prefix][short_name] = resource_number
                    advance_progress(progress, task_id)

    mismatched_entity: list[dict] = []
    for prefix, by_short in global_max_resource.items():
        prefix_counters = entity_counters.get(prefix, {})
        for short_name, max_resource in by_short.items():
            entity_counter = prefix_counters.get(short_name, 0)
            if entity_counter < max_resource:
                mismatched_entity.append({
                    "prefix": prefix,
                    "short_name": short_name,
                    "expected_min": max_resource,
                    "actual": entity_counter,
                })

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "root_path": os.path.abspath(root_path),
        "info_dir": os.path.abspath(info_dir),
        "total_zip_files": len(prov_zip_files),
        "total_mismatched_entity_counters": len(mismatched_entity),
        "total_mismatched_prov_counters": len(all_mismatched_prov),
        "mismatched_entity_counters": mismatched_entity,
        "mismatched_prov_counters": all_mismatched_prov,
    }

    output_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(orjson.dumps(report, option=orjson.OPT_INDENT_2))

    console.print(f"Mismatched entity counters: {len(mismatched_entity)}")
    console.print(f"Mismatched prov counters: {len(all_mismatched_prov)}")
    console.print(f"Report saved to {output_path}")


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Verify provenance entities have matching counter file entries.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("directory", type=str, help="Path to the RDF directory to scan")
    parser.add_argument("info_dir", type=str, help="Base directory for counter files")
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="check_info_dir_report.json",
        help="Output JSON report path (default: check_info_dir_report.json)",
    )
    args = parser.parse_args()
    explore_provenance_files(args.directory, args.info_dir, args.output)


if __name__ == "__main__":
    main()
