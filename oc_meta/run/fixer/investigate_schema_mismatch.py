#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
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

"""
Investigate identifier schema mismatches in RDF files.

This script reads RDF files directly (not via SPARQL) to find identifiers
where the declared schema does not match the value pattern.
"""

import json
import os
import re
import zipfile
from argparse import ArgumentParser
from collections import Counter
from datetime import datetime
from multiprocessing import Pool

from tqdm import tqdm

DATACITE_PREFIX = "http://purl.org/spar/datacite/"
LITERAL_VALUE_PROP = "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
SCHEME_PROP = "http://purl.org/spar/datacite/usesIdentifierScheme"

RA_SCHEMAS = {"crossref", "orcid", "viaf", "wikidata", "ror"}
BR_SCHEMAS = {"arxiv", "doi", "issn", "isbn", "jid", "openalex", "pmid", "pmcid", "url", "wikidata", "wikipedia"}
ALL_SCHEMAS = RA_SCHEMAS.union(BR_SCHEMAS)

SCHEMA_PATTERNS = {
    # RA schemas
    "orcid": re.compile(r"^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$"),
    "crossref": re.compile(r"^[0-9]+$"),
    "viaf": re.compile(r"^[0-9]+$"),
    "ror": re.compile(r"^0[a-z0-9]{8}$"),
    # BR schemas
    "doi": re.compile(r"^10\..+/.+$"),
    "issn": re.compile(r"^[0-9]{4}-[0-9]{3}[0-9X]$"),
    "isbn": re.compile(r"^(97[89])?[0-9]{9}[0-9X]$"),
    "pmid": re.compile(r"^[0-9]+$"),
    "pmcid": re.compile(r"^PMC[0-9]+$", re.IGNORECASE),
    "arxiv": re.compile(r"^([0-9]{4}\.[0-9]+|[a-z\-]+/[0-9]+)(v[0-9]+)?$", re.IGNORECASE),
    "url": re.compile(r"^https?://.+$", re.IGNORECASE),
    "jid": re.compile(r"^.+$"),
    "openalex": re.compile(r"^[WAISCV][0-9]+$"),
    # Shared schemas (both RA and BR)
    "wikidata": re.compile(r"^Q[0-9]+$"),
    "wikipedia": re.compile(r"^.+$"),
}

_missing_patterns = ALL_SCHEMAS - set(SCHEMA_PATTERNS.keys())
if _missing_patterns:
    raise RuntimeError(f"Missing validation patterns for schemas: {_missing_patterns}")


def is_identifier_node(node: dict) -> bool:
    """Check if node is an identifier node (has both schema and value)."""
    return SCHEME_PROP in node and LITERAL_VALUE_PROP in node


def extract_schema(node: dict) -> str:
    """Extract schema name from identifier node."""
    scheme_uri = node[SCHEME_PROP][0]["@id"]
    return scheme_uri.replace(DATACITE_PREFIX, "")


def extract_value(node: dict) -> str:
    """Extract literal value from identifier node."""
    return node[LITERAL_VALUE_PROP][0]["@value"]


def validate_value(schema: str, value: str) -> bool:
    """Check if value matches expected pattern for schema."""
    pattern = SCHEMA_PATTERNS[schema]
    return bool(pattern.match(value))


def process_zip_file(zip_path: str) -> dict:
    """Process a single ZIP file and return mismatches and unknown schemas."""
    result = {"mismatches": [], "unknown_schemas": []}
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for zip_info in zip_ref.infolist():
            if not zip_info.filename.endswith(".json"):
                continue
            with zip_ref.open(zip_info) as jsonfile:
                json_data = json.load(jsonfile)
                for item in json_data:
                    for node in item["@graph"]:
                        if not is_identifier_node(node):
                            continue
                        entity_id = node["@id"]
                        schema = extract_schema(node)
                        value = extract_value(node)
                        if schema not in ALL_SCHEMAS:
                            result["unknown_schemas"].append({
                                "id": entity_id,
                                "schema": schema,
                                "value": value,
                                "file": zip_path,
                            })
                        elif not validate_value(schema, value):
                            result["mismatches"].append({
                                "id": entity_id,
                                "schema": schema,
                                "value": value,
                                "file": zip_path,
                            })
    return result


def find_zip_files(rdf_dir: str) -> list:
    """Find all ZIP files in rdf/id/ directory, excluding prov folders."""
    id_dir = os.path.join(rdf_dir, "id")
    zip_files = []
    for root, _, files in os.walk(id_dir):
        if "prov" in root:
            continue
        for filename in files:
            if filename.endswith(".zip"):
                zip_files.append(os.path.join(root, filename))
    return zip_files


def main():
    parser = ArgumentParser(
        "investigate_schema_mismatch.py",
        description="Investigate identifier schema mismatches in RDF files",
    )
    parser.add_argument(
        "-r",
        "--rdf-dir",
        dest="rdf_dir",
        required=True,
        help="Path to RDF directory (containing id/, br/, ra/, etc.)",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        default="schema_mismatch_report.json",
        help="Output JSON file path (default: schema_mismatch_report.json)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        dest="workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )

    args = parser.parse_args()

    print("Finding ZIP files...")
    zip_files = find_zip_files(args.rdf_dir)
    print(f"Found {len(zip_files)} ZIP files to process")

    all_mismatches = []
    all_unknown_schemas = []
    mismatch_counts = Counter()
    unknown_schema_counts = Counter()

    print("Processing files...")
    with Pool(args.workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(process_zip_file, zip_files),
            total=len(zip_files),
            desc="Processing",
        ))

    for file_result in results:
        for mismatch in file_result["mismatches"]:
            all_mismatches.append(mismatch)
            mismatch_counts[mismatch["schema"]] += 1
        for unknown in file_result["unknown_schemas"]:
            all_unknown_schemas.append(unknown)
            unknown_schema_counts[unknown["schema"]] += 1

    report = {
        "timestamp": datetime.now().isoformat(),
        "rdf_dir": args.rdf_dir,
        "files_processed": len(zip_files),
        "total_mismatches": len(all_mismatches),
        "total_unknown_schemas": len(all_unknown_schemas),
        "mismatches_by_schema": mismatch_counts,
        "unknown_schemas_by_type": unknown_schema_counts,
        "mismatches": all_mismatches,
        "unknown_schemas": all_unknown_schemas,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 60)
    print("INVESTIGATION SUMMARY")
    print("=" * 60)
    print(f"Files processed: {len(zip_files)}")
    print(f"Total mismatches found: {len(all_mismatches)}")
    if mismatch_counts:
        print("Mismatches by schema:")
        for schema, count in sorted(mismatch_counts.items(), key=lambda x: -x[1]):
            print(f"  - {schema}: {count}")
    print(f"Total unknown schemas found: {len(all_unknown_schemas)}")
    if unknown_schema_counts:
        print("Unknown schemas:")
        for schema, count in sorted(unknown_schema_counts.items(), key=lambda x: -x[1]):
            print(f"  - {schema}: {count}")
    print(f"\nFull report saved to: {args.output}")
    print("=" * 60)


if __name__ == "__main__":
    main()
