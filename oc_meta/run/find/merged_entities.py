import argparse
import csv
import json
import os
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

import yaml
from rich.console import Console
from tqdm import tqdm

console = Console()

PROV_DERIVED_FROM = "http://www.w3.org/ns/prov#wasDerivedFrom"
PROV_SPECIALIZATION_OF = "http://www.w3.org/ns/prov#specializationOf"


def extract_entity_from_snapshot(snapshot_uri: str) -> str:
    return snapshot_uri.split("/prov/")[0]


def find_prov_files(rdf_dir: str, entity_type: str) -> list[str]:
    entity_dir = os.path.join(rdf_dir, entity_type)
    prov_files = []

    for root, _, files in os.walk(entity_dir):
        for f in files:
            if f == "se.zip":
                prov_files.append(os.path.join(root, f))

    return prov_files


def process_prov_file(prov_file: str) -> list[tuple[str, str]]:
    results = []

    try:
        with zipfile.ZipFile(prov_file, "r") as zf:
            with zf.open("se.json") as f:
                data = json.load(f)
    except (zipfile.BadZipFile, json.JSONDecodeError, KeyError):
        return results

    for graph in data:
        for entity in graph.get("@graph", []):
            derived_from = entity.get(PROV_DERIVED_FROM, [])
            if len(derived_from) < 2:
                continue

            specialization = entity.get(PROV_SPECIALIZATION_OF, [])
            if not specialization:
                continue

            surviving_entity = specialization[0]["@id"]

            for derived in derived_from:
                derived_snapshot = derived["@id"]
                derived_entity = extract_entity_from_snapshot(derived_snapshot)

                if derived_entity != surviving_entity:
                    results.append((surviving_entity, derived_entity))

    return results


def build_merge_graph(
    merge_results: list[tuple[str, str]],
) -> dict[str, str]:
    merged_to_surviving: dict[str, str] = {}

    for surviving, merged in merge_results:
        merged_to_surviving[merged] = surviving

    return merged_to_surviving


def find_final_surviving(entity: str, merged_to_surviving: dict[str, str]) -> str:
    current = entity
    visited = {entity}

    while current in merged_to_surviving:
        next_entity = merged_to_surviving[current]
        if next_entity in visited:
            break
        visited.add(next_entity)
        current = next_entity

    return current


def group_by_final_surviving(
    merged_to_surviving: dict[str, str],
) -> dict[str, list[str]]:
    final_to_merged: dict[str, list[str]] = defaultdict(list)

    for merged_entity in merged_to_surviving.keys():
        final = find_final_surviving(merged_entity, merged_to_surviving)
        final_to_merged[final].append(merged_entity)

    return dict(final_to_merged)


def main():
    parser = argparse.ArgumentParser(
        description="Find all merged entities and reconstruct merge chains from provenance files"
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to meta configuration YAML file"
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Output CSV file path"
    )
    parser.add_argument(
        "--entity-type",
        choices=["br", "ra", "id", "ar", "re"],
        required=True,
        help="Entity type to search",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    rdf_dir = os.path.join(config["output_rdf_dir"], "rdf")

    console.print(f"Scanning for provenance files in: {rdf_dir}/{args.entity_type}")
    prov_files = find_prov_files(rdf_dir, args.entity_type)
    console.print(f"Found {len(prov_files)} provenance files")

    all_results: list[tuple[str, str]] = []

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_prov_file, f): f for f in prov_files}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            results = future.result()
            all_results.extend(results)

    console.print(f"Found {len(all_results)} merge derivations")

    merged_to_surviving = build_merge_graph(all_results)
    console.print(f"Found {len(merged_to_surviving)} merged entities")

    final_to_merged = group_by_final_surviving(merged_to_surviving)
    console.print(f"Found {len(final_to_merged)} surviving entities")

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["surviving_entity", "merged_entities"])
        writer.writeheader()
        for surviving, merged_list in final_to_merged.items():
            writer.writerow({
                "surviving_entity": surviving,
                "merged_entities": "; ".join(merged_list),
            })

    console.print(f"Output written to: {args.output}")


if __name__ == "__main__":
    main()
