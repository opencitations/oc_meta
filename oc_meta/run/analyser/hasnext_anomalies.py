from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from multiprocessing import Pool
from typing import Dict, List, Optional, Tuple

import yaml
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from oc_meta.plugins.csv_generator_lite.csv_generator_lite import (
    find_file,
    load_json_from_file,
)

ROLE_MAP = {
    "http://purl.org/spar/pro/author": "author",
    "http://purl.org/spar/pro/editor": "editor",
    "http://purl.org/spar/pro/publisher": "publisher",
}
HAS_NEXT = "https://w3id.org/oc/ontology/hasNext"
IS_DOC_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
WITH_ROLE = "http://purl.org/spar/pro/withRole"
IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"

_worker_config: Optional[Tuple[str, int, int]] = None


def _init_worker(rdf_dir: str, dir_split_number: int, items_per_file: int) -> None:
    global _worker_config
    _worker_config = (rdf_dir, dir_split_number, items_per_file)


def _ar_summary(ar_uri: str, info: dict) -> dict:
    return {
        "ar": ar_uri,
        "ra": info["ra"],
        "has_next": info["has_next"],
    }


def load_ar_data(
    ar_uri: str, rdf_dir: str, dir_split_number: int, items_per_file: int
) -> Optional[dict]:
    ar_file = find_file(rdf_dir, dir_split_number, items_per_file, ar_uri)
    if not ar_file:
        return None
    data = load_json_from_file(ar_file)
    for graph in data:
        for entity in graph.get("@graph", []):
            if entity["@id"] == ar_uri:
                role_uri = ""
                if WITH_ROLE in entity:
                    role_uri = entity[WITH_ROLE][0]["@id"]
                role_type = ROLE_MAP.get(role_uri, "unknown")

                ra_uri = None
                if IS_HELD_BY in entity:
                    ra_uri = entity[IS_HELD_BY][0]["@id"]

                has_next = []
                if HAS_NEXT in entity:
                    has_next = [item["@id"] for item in entity[HAS_NEXT]]

                return {
                    "role_type": role_type,
                    "ra": ra_uri,
                    "has_next": has_next,
                }
    return None


def detect_cycles(ar_data: Dict[str, dict], ar_uris_in_group: set) -> List[List[str]]:
    adj: Dict[str, List[str]] = {}
    for ar_uri, info in ar_data.items():
        targets = [
            t for t in info["has_next"] if t in ar_uris_in_group and t != ar_uri
        ]
        if targets:
            adj[ar_uri] = targets

    globally_visited: set = set()
    cycles: List[List[str]] = []

    for start in ar_uris_in_group:
        if start in globally_visited:
            continue

        path: List[str] = []
        path_set: set = set()
        stack: List[Tuple[str, int]] = [(start, -1)]

        while stack:
            node, ni = stack[-1]

            if ni == -1:
                if node in path_set:
                    cycle_start = path.index(node)
                    cycles.append(list(path[cycle_start:]))
                    stack.pop()
                    continue
                if node in globally_visited:
                    stack.pop()
                    continue
                path.append(node)
                path_set.add(node)
                stack[-1] = (node, 0)
                continue

            neighbors = adj.get(node, [])
            if ni < len(neighbors):
                stack[-1] = (node, ni + 1)
                stack.append((neighbors[ni], -1))
            else:
                path.pop()
                path_set.discard(node)
                globally_visited.add(node)
                stack.pop()

    return cycles


def find_anomalies(
    br_uri: str, role_type: str, ar_data: Dict[str, dict]
) -> List[dict]:
    anomalies: List[dict] = []
    ar_uris_in_group = set(ar_data.keys())

    for ar_uri, info in ar_data.items():
        if ar_uri in info["has_next"]:
            anomalies.append({
                "anomaly_type": "self_loop",
                "br": br_uri,
                "role_type": role_type,
                "ars_involved": [_ar_summary(ar_uri, info)],
                "details": f"AR {ar_uri.split('/')[-1]} hasNext points to itself",
            })

    for ar_uri, info in ar_data.items():
        if len(info["has_next"]) > 1:
            anomalies.append({
                "anomaly_type": "multiple_has_next",
                "br": br_uri,
                "role_type": role_type,
                "ars_involved": [_ar_summary(ar_uri, info)],
                "details": (
                    f"AR {ar_uri.split('/')[-1]} has"
                    f" {len(info['has_next'])} hasNext targets"
                ),
            })

    for ar_uri, info in ar_data.items():
        for target in info["has_next"]:
            if target not in ar_uris_in_group:
                anomalies.append({
                    "anomaly_type": "dangling_has_next",
                    "br": br_uri,
                    "role_type": role_type,
                    "ars_involved": [_ar_summary(ar_uri, info)],
                    "details": (
                        f"AR {ar_uri.split('/')[-1]} hasNext points to"
                        f" {target.split('/')[-1]} which is not in this"
                        " BR/role group"
                    ),
                })

    referenced_ars = set()
    for info in ar_data.values():
        for target in info["has_next"]:
            if target in ar_uris_in_group:
                referenced_ars.add(target)

    start_nodes = [ar for ar in ar_uris_in_group if ar not in referenced_ars]

    if len(ar_data) > 1:
        if len(start_nodes) == 0:
            anomalies.append({
                "anomaly_type": "no_start_node",
                "br": br_uri,
                "role_type": role_type,
                "ars_involved": [
                    _ar_summary(ar_uri, ar_data[ar_uri]) for ar_uri in ar_data
                ],
                "details": (
                    f"All {len(ar_data)} ARs are targets of hasNext"
                    " (fully circular)"
                ),
            })
        elif len(start_nodes) > 1:
            anomalies.append({
                "anomaly_type": "multiple_start_nodes",
                "br": br_uri,
                "role_type": role_type,
                "ars_involved": [
                    _ar_summary(ar_uri, ar_data[ar_uri]) for ar_uri in start_nodes
                ],
                "details": (
                    f"{len(start_nodes)} ARs have no incoming hasNext"
                    " (disconnected fragments)"
                ),
            })

    cycles = detect_cycles(ar_data, ar_uris_in_group)
    for cycle in cycles:
        cycle_ids = [uri.split("/")[-1] for uri in cycle]
        anomalies.append({
            "anomaly_type": "cycle",
            "br": br_uri,
            "role_type": role_type,
            "ars_involved": [
                _ar_summary(ar_uri, ar_data[ar_uri]) for ar_uri in cycle
            ],
            "details": (
                f"{len(cycle)}-node cycle:"
                f" {' -> '.join(cycle_ids)} -> {cycle_ids[0]}"
            ),
        })

    return anomalies


def _detect_anomalies_in_file(filepath: str) -> Tuple[str, int, List[dict]]:
    rdf_dir, dir_split_number, items_per_file = _worker_config
    anomalies: List[dict] = []
    br_count = 0
    data = load_json_from_file(filepath)
    for graph in data:
        for entity in graph.get("@graph", []):
            if IS_DOC_CONTEXT_FOR not in entity:
                continue
            br_count += 1
            br_uri = entity["@id"]

            ar_uris = [ar["@id"] for ar in entity[IS_DOC_CONTEXT_FOR]]
            ar_data: Dict[str, dict] = {}
            for ar_uri in ar_uris:
                info = load_ar_data(
                    ar_uri, rdf_dir, dir_split_number, items_per_file
                )
                if info:
                    ar_data[ar_uri] = info

            role_groups: Dict[str, Dict[str, dict]] = {}
            for ar_uri, info in ar_data.items():
                role = info["role_type"]
                if role not in role_groups:
                    role_groups[role] = {}
                role_groups[role][ar_uri] = info

            for role_type, group in role_groups.items():
                anomalies.extend(find_anomalies(br_uri, role_type, group))

    return (filepath, br_count, anomalies)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect hasNext chain anomalies in RDF data"
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Meta config YAML file path"
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Output JSON report path"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    rdf_dir = os.path.join(settings["output_rdf_dir"], "rdf")
    dir_split_number = settings["dir_split_number"]
    items_per_file = settings["items_per_file"]

    br_dir = os.path.join(rdf_dir, "br")
    if not os.path.exists(br_dir):
        print(f"Error: BR directory not found at {br_dir}")
        return

    all_files = []
    for root, _, files in os.walk(br_dir):
        if "prov" in root:
            continue
        all_files.extend(
            os.path.join(root, f) for f in files if f.endswith(".zip")
        )
    all_files = sorted(all_files)

    if not all_files:
        print("No BR zip files found")
        return

    print(
        f"Processing {len(all_files)} BR files with {args.workers} workers..."
    )

    total_brs = 0
    all_anomalies: List[dict] = []

    with Pool(
        args.workers,
        _init_worker,
        (rdf_dir, dir_split_number, items_per_file),
    ) as pool:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(
                "Scanning for anomalies", total=len(all_files)
            )
            for filepath, br_count, anomalies in pool.imap_unordered(
                _detect_anomalies_in_file, all_files
            ):
                total_brs += br_count
                all_anomalies.extend(anomalies)
                progress.update(task, advance=1)

    anomalies_by_type = dict(
        Counter(a["anomaly_type"] for a in all_anomalies)
    )

    report = {
        "config": os.path.abspath(args.config),
        "rdf_dir": rdf_dir,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_brs_analyzed": total_brs,
        "total_anomalies": len(all_anomalies),
        "anomalies_by_type": anomalies_by_type,
        "anomalies": all_anomalies,
    }

    output_dir = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(output_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Analyzed {total_brs} BRs, found {len(all_anomalies)} anomalies")
    for atype, count in sorted(anomalies_by_type.items()):
        print(f"  {atype}: {count}")
    print(f"Report saved to {args.output}")


if __name__ == "__main__":
    main()
