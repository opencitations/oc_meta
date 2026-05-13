# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import os
from argparse import ArgumentParser

from rich_argparse import RichHelpFormatter

from oc_meta.lib.cleaner import normalize_hyphens, normalize_id
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import get_csv_data, write_csv

CITING_COL = "citing_id"
CITED_COL = "cited_id"


def _csv_files_in_dir(directory: str) -> list[str]:
    return sorted(
        os.path.join(directory, f.name)
        for f in os.scandir(directory)
        if f.name.endswith(".csv") and f.is_file()
    )


def _count_csv_rows(filepath: str) -> int:
    with open(filepath, "rb") as f:
        return sum(1 for _ in f) - 1


def build_id_to_omid_mapping(meta_output_dir: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    csv_files = _csv_files_in_dir(meta_output_dir)
    for i, csv_path in enumerate(csv_files, 1):
        console.print(
            f"  Reading file {i}/{len(csv_files)}: {os.path.basename(csv_path)}"
        )
        for row in get_csv_data(csv_path):
            parts = row["id"].strip().split()
            omids = [p for p in parts if p.startswith("omid:")]
            others = [p for p in parts if not p.startswith("omid:")]
            for omid in omids:
                for other in others:
                    mapping[other.lower()] = omid
    return mapping


def convert_citations(
    meta_output_dir: str,
    citations_dir: str,
    output_dir: str,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    citation_files = _csv_files_in_dir(citations_dir)
    total_cit_rows = sum(_count_csv_rows(f) for f in citation_files)

    total_resolved = 0
    total_unresolved_citing = 0
    total_unresolved_cited = 0
    total_invalid = 0
    all_orphan_ids: set[str] = set()

    console.print("[bold]Building ID→OMID mapping…[/bold]")
    mapping = build_id_to_omid_mapping(meta_output_dir)
    console.print(f"  Loaded {len(mapping)} ID→OMID entries")

    with create_progress() as progress:
        cit_task = progress.add_task("Converting citations", total=total_cit_rows)
        for cit_path in citation_files:
            out_path = os.path.join(output_dir, os.path.basename(cit_path))

            resolved = 0
            unresolved_citing = 0
            unresolved_cited = 0
            output_rows: list[dict[str, str]] = []

            for row in get_csv_data(cit_path):
                raw_citing = row[CITING_COL].strip()
                raw_cited = row[CITED_COL].strip()

                citing_id = normalize_id(normalize_hyphens(raw_citing))
                cited_id = normalize_id(normalize_hyphens(raw_cited))

                if not citing_id or not cited_id:
                    total_invalid += 1
                    progress.advance(cit_task)
                    continue

                citing_omid = mapping.get(citing_id.lower())
                cited_omid = mapping.get(cited_id.lower())

                if citing_omid and cited_omid:
                    output_rows.append({"citing": citing_omid, "cited": cited_omid})
                    resolved += 1
                else:
                    if not citing_omid:
                        unresolved_citing += 1
                        all_orphan_ids.add(citing_id)
                    if not cited_omid:
                        unresolved_cited += 1
                        all_orphan_ids.add(cited_id)
                progress.advance(cit_task)

            write_csv(out_path, output_rows)
            total_resolved += resolved
            total_unresolved_citing += unresolved_citing
            total_unresolved_cited += unresolved_cited

    console.print(f"\n[bold]Results:[/bold]")
    console.print(f"  Total citations:     {total_cit_rows}")
    console.print(f"  Resolved:            {total_resolved}")
    console.print(f"  Invalid IDs:         {total_invalid}")
    console.print(f"  Unresolved citing:   {total_unresolved_citing}")
    console.print(f"  Unresolved cited:    {total_unresolved_cited}")
    console.print(f"  Unique orphan IDs:   {len(all_orphan_ids)}")


if __name__ == "__main__":  # pragma: no cover
    arg_parser = ArgumentParser(
        "convert_citations.py",
        description="Convert citation CSVs by replacing source IDs with OMIDs from Meta output CSVs. "
        "Both --meta-output and --citations take directory paths. "
        "Validates transitive closure and reports any unresolvable citation IDs.",
        formatter_class=RichHelpFormatter,
    )
    arg_parser.add_argument(
        "-m",
        "--meta-output",
        required=True,
        help="Directory containing Meta output CSVs",
    )
    arg_parser.add_argument(
        "-c",
        "--citations",
        required=True,
        help="Directory containing input citation CSVs",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Directory for output citation CSVs (with 'citing' and 'cited' columns)",
    )
    args = arg_parser.parse_args()
    convert_citations(
        meta_output_dir=args.meta_output,
        citations_dir=args.citations,
        output_dir=args.output,
    )
