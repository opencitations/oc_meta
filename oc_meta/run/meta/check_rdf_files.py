# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

"""Disk-only verification of MetaProcess RDF output over the whole curated CSV.

Unlike ``check_results.py`` this script never queries the triplestore: it is meant
for ``rdf_files_only`` runs, where the database is still stale. It scans every row of
the produced curated CSV (and optionally cross-checks the original input) and verifies
every referenced entity against the JSON-LD data and provenance zip files on disk.

Rows are streamed and results aggregated incrementally with a bounded in-flight window,
so neither the CSV, the worker futures, nor the per-row results are held in memory at
once.
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing
import os
import sys
import traceback
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import orjson
import yaml
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich_argparse import RichHelpFormatter

from oc_meta.core.creator import Creator
from oc_meta.lib.cleaner import normalize_id
from oc_meta.lib.console import console
from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.find.hasnext_anomalies import (
    HAS_NEXT,
    IS_DOC_CONTEXT_FOR,
    find_anomalies,
)
from oc_meta.run.meta.check_results import _extract_entity_groups, find_prov_file
from oc_meta.run.meta.generate_csv import URI_TYPE_DICT, load_json_from_file

TITLE = "http://purl.org/dc/terms/title"
PUB_DATE = "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
HAS_IDENTIFIER = "http://purl.org/spar/datacite/hasIdentifier"
USES_SCHEME = "http://purl.org/spar/datacite/usesIdentifierScheme"
HAS_LITERAL = "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
WITH_ROLE = "http://purl.org/spar/pro/withRole"
IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"
SPECIALIZATION_OF = "http://www.w3.org/ns/prov#specializationOf"
INVALIDATED = "http://www.w3.org/ns/prov#invalidatedAtTime"
DATACITE_PREFIX = "http://purl.org/spar/datacite/"

AGENT_COLUMNS = ("author", "editor", "publisher")
CHECKS = ("data_graph", "identifier", "metadata", "agents", "provenance", "hasnext")

_config: Optional[tuple[str, int, int]] = None
_index: Optional[dict[str, str]] = None


@dataclass
class RowResult:
    counts: Counter = field(default_factory=Counter)
    errors: list[dict] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)


class EntityCache:
    """Per-row cache of data files, so one zip is read once even for many entities."""

    def __init__(self, rdf_dir: str, dir_split: int, items_per_file: int) -> None:
        self.rdf_dir = rdf_dir
        self.dir_split = dir_split
        self.items_per_file = items_per_file
        self._files: dict[str, Optional[dict[str, dict]]] = {}

    def _graphs(self, data_zip: str) -> Optional[dict[str, dict]]:
        if data_zip not in self._files:
            if not os.path.exists(data_zip):
                self._files[data_zip] = None
            else:
                index: dict[str, dict] = {}
                for graph in load_json_from_file(data_zip):
                    for entity in graph["@graph"]:
                        index[entity["@id"]] = entity
                self._files[data_zip] = index
        return self._files[data_zip]

    def data_file(self, uri: str) -> str:
        return find_rdf_file(
            uri, self.rdf_dir, self.dir_split, self.items_per_file, zip_output=True
        )

    def get(self, uri: str) -> Optional[dict]:
        graphs = self._graphs(self.data_file(uri))
        if graphs is None:
            return None
        return graphs.get(uri)


def _values(entity: dict, predicate: str) -> list[str]:
    return [item["@value"] for item in entity.get(predicate, [])]


def _ids(entity: dict, predicate: str) -> list[str]:
    return [item["@id"] for item in entity.get(predicate, [])]


def _entity_type_string(entity: dict) -> Optional[str]:
    mapped = [URI_TYPE_DICT[t] for t in entity.get("@type", []) if t in URI_TYPE_DICT]
    specific = [m for m in mapped if m]
    if specific:
        return specific[0]
    return "" if mapped else None


def _canonical_type(label: Optional[str]) -> Optional[str]:
    """Collapse synonymous type labels (e.g. 'data file' / 'dataset') to one form.

    The curated CSV and the fabio class can carry different labels for the same type,
    so both sides are mapped through the creator's type vocabulary before comparison.
    """
    if not label:
        return label
    return Creator._TYPE_TO_METHOD.get(label, label)


def _linked_identifiers(br: dict, cache: EntityCache) -> set[str]:
    keys: set[str] = set()
    for id_uri in _ids(br, HAS_IDENTIFIER):
        id_entity = cache.get(id_uri)
        if id_entity is None:
            continue
        schemes = _ids(id_entity, USES_SCHEME)
        values = _values(id_entity, HAS_LITERAL)
        if not schemes or not values:
            continue
        scheme = schemes[0]
        scheme = (
            scheme[len(DATACITE_PREFIX) :]
            if scheme.startswith(DATACITE_PREFIX)
            else scheme
        )
        normalized = normalize_id(f"{scheme}:{values[0]}")
        if normalized:
            keys.add(normalized)
    return keys


def _ar_chain_order(ar_data: dict[str, dict], role_uri: str) -> Optional[list[str]]:
    """Return the RA sequence following hasNext, or None if the chain is malformed."""
    members = {ar: info for ar, info in ar_data.items() if info["role_uri"] == role_uri}
    if not members:
        return []
    targets = {t for info in members.values() for t in info["has_next"] if t in members}
    starts = [ar for ar in members if ar not in targets]
    if len(starts) != 1:
        return None
    order: list[str] = []
    seen: set[str] = set()
    current: Optional[str] = starts[0]
    while current is not None:
        if current in seen:
            return None
        seen.add(current)
        order.append(members[current]["ra"])
        nexts = [t for t in members[current]["has_next"] if t in members]
        if len(nexts) > 1:
            return None
        current = nexts[0] if nexts else None
    if len(seen) != len(members):
        return None
    return order


def _check_provenance(uri: str, cache: EntityCache) -> Optional[str]:
    """Return an error subtype, or None if the latest snapshot exists and is valid."""
    prov_file = find_prov_file(cache.data_file(uri))
    if prov_file is None:
        return "provenance_missing"
    snapshots: dict[int, dict] = {}
    for graph in load_json_from_file(prov_file):
        for snapshot in graph["@graph"]:
            if uri in _ids(snapshot, SPECIALIZATION_OF):
                number = int(snapshot["@id"].rsplit("/se/", 1)[1])
                snapshots[number] = snapshot
    if not snapshots:
        return "provenance_missing"
    if INVALIDATED in snapshots[max(snapshots)]:
        return "provenance_invalidated"
    return None


def check_curated_row(
    row: dict, row_num: int, cache: EntityCache, base_iri: str
) -> RowResult:
    result = RowResult()

    id_group = _extract_entity_groups(row["id"], "id", base_iri)[0]
    br_uri = id_group["omid_uri"]
    if not br_uri:
        result.warnings.append({"type": "row_without_omid", "row": row_num})
        return result

    ra_by_role: dict[str, list[str]] = {}
    entity_uris = {br_uri}
    id_groups_by_uri = {br_uri: id_group}
    for col in AGENT_COLUMNS:
        for group in _extract_entity_groups(row[col], col, base_iri):
            if group["omid_uri"]:
                ra_by_role.setdefault(col, []).append(group["omid_uri"])
                entity_uris.add(group["omid_uri"])
    for group in _extract_entity_groups(row["venue"], "venue", base_iri):
        if group["omid_uri"]:
            entity_uris.add(group["omid_uri"])
            id_groups_by_uri[group["omid_uri"]] = group

    # A. data presence
    present: dict[str, Optional[dict]] = {}
    for uri in entity_uris:
        result.counts["data_graph.checked"] += 1
        entity = cache.get(uri)
        present[uri] = entity
        if entity is None:
            result.counts["data_graph.failed"] += 1
            result.errors.append(
                {
                    "type": "data_graph_missing",
                    "omid": uri,
                    "file": cache.data_file(uri),
                    "row": row_num,
                }
            )

    # B. identifier linkage (br + venue)
    for uri, group in id_groups_by_uri.items():
        entity = present.get(uri)
        if entity is None:
            continue
        linked = _linked_identifiers(entity, cache)
        for schema, value in group["recognized"]:
            normalized = normalize_id(f"{schema}:{value}")
            if not normalized:
                continue
            result.counts["identifier.checked"] += 1
            if normalized not in linked:
                result.counts["identifier.failed"] += 1
                result.errors.append(
                    {
                        "type": "identifier_not_linked",
                        "omid": uri,
                        "identifier": normalized,
                        "linked": sorted(linked),
                        "row": row_num,
                    }
                )

    br = present[br_uri]
    if br is not None:
        _check_metadata(row, row_num, br, result)
        _check_agents_and_chain(row_num, br, ra_by_role, cache, result)

    # D. provenance presence
    for uri in entity_uris:
        if present.get(uri) is None:
            continue
        result.counts["provenance.checked"] += 1
        problem = _check_provenance(uri, cache)
        if problem == "provenance_missing":
            result.counts["provenance.failed"] += 1
            result.errors.append({"type": problem, "omid": uri, "row": row_num})
        elif problem == "provenance_invalidated":
            # entity referenced by the run but its latest snapshot is invalidated
            # (typically a pre-existing merge/deletion leftover, not a fault of this run)
            result.counts["provenance.invalidated"] += 1
            result.warnings.append({"type": problem, "omid": uri, "row": row_num})

    return result


def _check_metadata(row: dict, row_num: int, br: dict, result: RowResult) -> None:
    result.counts["metadata.checked"] += 1
    failed = False

    title = row["title"].strip()
    rdf_title = _values(br, TITLE)
    if title and (not rdf_title or rdf_title[0].casefold() != title.casefold()):
        failed = True
        result.errors.append(
            {
                "type": "metadata_mismatch",
                "subtype": "title",
                "omid": br["@id"],
                "csv": title,
                "rdf": rdf_title,
                "row": row_num,
            }
        )

    pub_date = row["pub_date"].strip()
    rdf_date = _values(br, PUB_DATE)
    if pub_date and (not rdf_date or rdf_date[0] != pub_date):
        failed = True
        result.errors.append(
            {
                "type": "metadata_mismatch",
                "subtype": "pub_date",
                "omid": br["@id"],
                "csv": pub_date,
                "rdf": rdf_date,
                "row": row_num,
            }
        )

    type_str = row["type"].strip().lower()
    rdf_type = _entity_type_string(br)
    if type_str and _canonical_type(type_str) != _canonical_type(rdf_type):
        failed = True
        result.errors.append(
            {
                "type": "metadata_mismatch",
                "subtype": "type",
                "omid": br["@id"],
                "csv": type_str,
                "rdf": rdf_type,
                "row": row_num,
            }
        )

    if failed:
        result.counts["metadata.failed"] += 1


def _load_ar_group(br: dict, cache: EntityCache) -> dict[str, dict]:
    ar_data: dict[str, dict] = {}
    for ar_uri in _ids(br, IS_DOC_CONTEXT_FOR):
        ar = cache.get(ar_uri)
        if ar is None:
            continue
        ar_data[ar_uri] = {
            "role_uri": (_ids(ar, WITH_ROLE) or [""])[0],
            "ra": (_ids(ar, IS_HELD_BY) or [None])[0],
            "has_next": _ids(ar, HAS_NEXT),
        }
    return ar_data


def _check_agents_and_chain(
    row_num: int,
    br: dict,
    ra_by_role: dict[str, list[str]],
    cache: EntityCache,
    result: RowResult,
) -> None:
    ar_data = _load_ar_group(br, cache)

    # E. structural anomalies (the regression check for the duplicate-RA fix)
    result.counts["hasnext.checked"] += 1
    role_groups: dict[str, dict[str, dict]] = {}
    for ar_uri, info in ar_data.items():
        role = info["role_uri"].rsplit("/", 1)[-1] if info["role_uri"] else "unknown"
        role_groups.setdefault(role, {})[ar_uri] = {
            "ra": info["ra"],
            "has_next": info["has_next"],
        }
    anomalies = []
    for role, group in role_groups.items():
        anomalies.extend(find_anomalies(br["@id"], role, group))
    if anomalies:
        result.counts["hasnext.failed"] += 1
        for anomaly in anomalies:
            result.errors.append(
                {
                    "type": "hasnext_anomaly",
                    "anomaly_type": anomaly["anomaly_type"],
                    "omid": br["@id"],
                    "details": anomaly["details"],
                    "row": row_num,
                }
            )

    # Orphan agent roles (withRole but no isHeldBy) are a pre-existing structural
    # leftover, usually from an old merge/deletion. They are excluded from the agent
    # comparison below and reported separately as warnings.
    for ar_uri, info in ar_data.items():
        if info["ra"] is None:
            result.counts["agents.orphan"] += 1
            result.warnings.append(
                {
                    "type": "ar_without_agent",
                    "omid": br["@id"],
                    "ar": ar_uri,
                    "role": info["role_uri"].rsplit("/", 1)[-1] or "unknown",
                    "row": row_num,
                }
            )

    # C. agents set + order vs curated CSV (real agents only)
    result.counts["agents.checked"] += 1
    agents_failed = False
    for col, expected in ra_by_role.items():
        role_uri = f"http://purl.org/spar/pro/{col}"
        rdf_ras = [
            info["ra"]
            for info in ar_data.values()
            if info["role_uri"] == role_uri and info["ra"] is not None
        ]
        if set(expected) != set(rdf_ras):
            agents_failed = True
            result.errors.append(
                {
                    "type": "agents_mismatch",
                    "subtype": col,
                    "omid": br["@id"],
                    "csv": expected,
                    "rdf": rdf_ras,
                    "row": row_num,
                }
            )
            continue
        order = _ar_chain_order(ar_data, role_uri)
        if order is not None:
            order = [ra for ra in order if ra is not None]
            if order != expected:
                agents_failed = True
                result.errors.append(
                    {
                        "type": "agent_order_mismatch",
                        "subtype": col,
                        "omid": br["@id"],
                        "csv": expected,
                        "rdf": order,
                        "row": row_num,
                    }
                )
    if agents_failed:
        result.counts["agents.failed"] += 1


def check_input_row(row: dict, row_num: int, base_iri: str) -> RowResult:
    """Verify each input row is represented in the curated output (no dropped rows).

    Title/metadata fidelity is not compared here: the input text is raw while the RDF
    holds the fully cleaned form (capitalisation, HTML stripping, ...), so a textual
    diff is pure noise. Fidelity is covered by check C against the curated CSV.
    """
    assert _index is not None
    result = RowResult()
    result.counts["input.checked"] += 1

    id_group = _extract_entity_groups(row["id"], "id", base_iri)[0]
    recognized = [normalize_id(f"{s}:{v}") for s, v in id_group["recognized"]]
    recognized = [r for r in recognized if r]
    if not recognized:
        result.counts["input.unverifiable"] += 1
        return result

    omids = {_index[r] for r in recognized if r in _index}
    if not omids:
        result.counts["input.failed"] += 1
        result.errors.append(
            {
                "type": "input_row_dropped",
                "identifiers": recognized,
                "row": row_num,
            }
        )
    elif len(omids) > 1:
        result.warnings.append(
            {
                "type": "input_multiple_omids",
                "identifiers": recognized,
                "omids": sorted(omids),
                "row": row_num,
            }
        )
    return result


def _init_worker(rdf_dir: str, dir_split: int, items_per_file: int) -> None:
    global _config
    _config = (rdf_dir, dir_split, items_per_file)


def _run_curated(args: tuple) -> RowResult:
    row, row_num, base_iri = args
    assert _config is not None
    cache = EntityCache(*_config)
    try:
        return check_curated_row(row, row_num, cache, base_iri)
    except Exception:  # noqa: BLE001 - record the row and keep scanning
        result = RowResult()
        result.counts["row_exception"] += 1
        result.errors.append(
            {
                "type": "row_check_exception",
                "row": row_num,
                "traceback": traceback.format_exc(),
            }
        )
        return result


def _text_lines(f):
    for raw in f:
        yield raw.decode("utf-8", errors="replace")


def _rows(csv_path: str) -> Iterator[tuple[dict, int]]:
    with open(csv_path, "rb") as f:
        reader = csv.DictReader(line.replace("\0", "") for line in _text_lines(f))
        for i, row in enumerate(reader):
            yield row, i + 1


def _iter_curated(
    csv_path: str, base_iri: str, index: Optional[dict[str, str]]
) -> Iterator[tuple[dict, int]]:
    """Stream curated rows, building the id->omid index in the same pass when needed."""
    for row, row_num in _rows(csv_path):
        if index is not None:
            group = _extract_entity_groups(row["id"], "id", base_iri)[0]
            if group["omid_uri"]:
                for schema, value in group["recognized"]:
                    normalized = normalize_id(f"{schema}:{value}")
                    if normalized:
                        index[normalized] = group["omid_uri"]
        yield row, row_num


def _count_progress() -> Progress:
    """Progress that shows a rising count, with no total (rows are not pre-counted)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("[cyan]{task.completed}[/cyan]"),
        TimeElapsedColumn(),
        console=console,
    )


def _drive(
    executor, submit, items, description: str, workers: int
) -> tuple[Counter, list[dict], list[dict]]:
    """Run ``submit`` over a stream of ``items`` with a bounded in-flight window.

    Results are folded incrementally so neither the futures nor the per-row results
    accumulate in memory.
    """
    counts: Counter = Counter()
    errors: list[dict] = []
    warnings: list[dict] = []
    max_in_flight = workers * 4
    in_flight: set = set()

    with _count_progress() as progress:
        task = progress.add_task(description)

        def fold(futures) -> None:
            for future in futures:
                result = future.result()
                counts.update(result.counts)
                errors.extend(result.errors)
                warnings.extend(result.warnings)
                progress.advance(task)

        for item in items:
            in_flight.add(submit(executor, item))
            if len(in_flight) >= max_in_flight:
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                fold(done)
        fold(as_completed(in_flight))

    return counts, errors, warnings


def _merge(
    into: tuple[Counter, list[dict], list[dict]],
    other: tuple[Counter, list[dict], list[dict]],
) -> None:
    into[0].update(other[0])
    into[1].extend(other[1])
    into[2].extend(other[2])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check MetaProcess RDF files (data + provenance) on disk",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta config YAML path")
    parser.add_argument(
        "--csv", required=True, help="Curated output CSV produced by the run"
    )
    parser.add_argument(
        "--input-csv", help="Original input CSV for the input cross-check"
    )
    parser.add_argument("-o", "--output", required=True, help="Output JSON report path")
    parser.add_argument(
        "--workers", type=int, default=8, help="Parallel workers (default: 8)"
    )
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        settings = yaml.safe_load(f)
    rdf_dir = os.path.join(settings["output_rdf_dir"], "rdf")
    base_iri = settings["base_iri"]
    config = (rdf_dir, settings["dir_split_number"], settings["items_per_file"])

    index: Optional[dict[str, str]] = {} if args.input_csv else None

    console.print(f"Checking every row of {os.path.basename(args.csv)}...")
    with ProcessPoolExecutor(
        max_workers=args.workers,
        initializer=_init_worker,
        initargs=config,
        mp_context=multiprocessing.get_context("forkserver"),
    ) as pool:
        aggregated = _drive(
            pool,
            lambda ex, item: ex.submit(_run_curated, (item[0], item[1], base_iri)),
            _iter_curated(args.csv, base_iri, index),
            "Checking curated rows",
            args.workers,
        )

    if args.input_csv:
        assert index is not None
        global _index
        _index = index
        console.print(
            f"Cross-checking every row of {os.path.basename(args.input_csv)}..."
        )
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            _merge(
                aggregated,
                _drive(
                    pool,
                    lambda ex, item: ex.submit(
                        check_input_row, item[0], item[1], base_iri
                    ),
                    _rows(args.input_csv),
                    "Checking input rows",
                    args.workers,
                ),
            )

    counts, errors, warnings = aggregated
    status = "PASS" if not errors else "FAIL"
    summary = {
        check: {
            "checked": counts[f"{check}.checked"],
            "failed": counts[f"{check}.failed"],
        }
        for check in CHECKS
    }
    summary["provenance"]["invalidated"] = counts["provenance.invalidated"]
    summary["input"] = {
        "checked": counts["input.checked"],
        "failed": counts["input.failed"],
        "unverifiable": counts["input.unverifiable"],
    }

    report = {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "config_path": os.path.abspath(args.config),
        "csv": os.path.abspath(args.csv),
        "input_csv": os.path.abspath(args.input_csv) if args.input_csv else None,
        "summary": summary,
        "errors_total": len(errors),
        "warnings_total": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
    with open(args.output, "wb") as f:
        f.write(orjson.dumps(report, option=orjson.OPT_INDENT_2))

    console.print(f"\nStatus: [bold]{status}[/bold]")
    for check, stats in summary.items():
        console.print(
            f"  {check:11} checked={stats['checked']} failed={stats.get('failed', 0)}"
        )
    console.print(f"Errors: {len(errors)}, Warnings: {len(warnings)}")
    console.print(f"Report saved to {args.output}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
