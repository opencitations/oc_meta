# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import signal
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from zipfile import ZipFile

import orjson
import yaml
from oc_ocdm.graph import GraphSet
from oc_ocdm.support import get_prefix
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import collect_files, collect_zip_files, find_rdf_file

FRBR_PART_OF = "http://purl.org/vocab/frbr/core#partOf"
HAS_IDENTIFIER = "http://purl.org/spar/datacite/hasIdentifier"
USES_ID_SCHEME = "http://purl.org/spar/datacite/usesIdentifierScheme"
HAS_LITERAL_VALUE = (
    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
)
DCTERMS_TITLE = "http://purl.org/dc/terms/title"
FABIO_EXPRESSION = "http://purl.org/spar/fabio/Expression"

BATCH_SIZE = 100

_stop_requested = False


def _worker_init() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _handle_signal(_signum: int, _frame: object) -> None:
    global _stop_requested
    _stop_requested = True
    console.print("[yellow]Interrupt received, finishing current entity...[/yellow]")


def _iter_entities(files: list, zip_output: bool):
    for fpath in files:
        if zip_output:
            with ZipFile(fpath, "r") as zf:
                data = orjson.loads(zf.read(zf.namelist()[0]))
        else:
            with open(fpath, "rb") as f:
                data = orjson.loads(f.read())
        for graph in data:
            for entity in graph.get("@graph", []):
                yield entity


def _read_entity(
    uri: str,
    rdf_dir: str,
    dir_split: int,
    items_per_file: int,
    zip_output: bool,
) -> dict | None:
    fpath = find_rdf_file(uri, rdf_dir, dir_split, items_per_file, zip_output)
    if not os.path.exists(fpath):
        return None
    if zip_output:
        with ZipFile(fpath, "r") as zf:
            data = orjson.loads(zf.read(zf.namelist()[0]))
    else:
        with open(fpath, "rb") as f:
            data = orjson.loads(f.read())
    for graph in data:
        for entity in graph.get("@graph", []):
            if entity["@id"] == uri:
                return entity
    return None


@dataclass
class ResolvedCase:
    br_uri: str
    correct_part_of: str | None
    to_remove: list[str]
    method: str
    reason: str


# ── Phase 1: Scan ──


def _scan_br_batch(
    files: list[str], zip_output: bool
) -> list[tuple[str, list[str]]]:
    results: list[tuple[str, list[str]]] = []
    for entity in _iter_entities(files, zip_output):
        if FRBR_PART_OF in entity:
            parents = entity[FRBR_PART_OF]
            if len(parents) > 1:
                results.append((entity["@id"], [p["@id"] for p in parents]))
    return results


def scan_duplicate_part_of(
    rdf_dir: str,
    zip_output: bool,
    workers: int = 4,
    batch_size: int = BATCH_SIZE,
) -> list[tuple[str, list[str]]]:
    br_dir = os.path.join(rdf_dir, "br")
    if zip_output:
        br_files = collect_zip_files(br_dir, only_data=True)
    else:
        br_files = collect_files(br_dir, "*.json", lambda p: "prov" not in p)

    batches = [
        br_files[i : i + batch_size] for i in range(0, len(br_files), batch_size)
    ]
    all_results: list[tuple[str, list[str]]] = []

    ctx = multiprocessing.get_context("forkserver")
    with create_progress() as progress:
        task = progress.add_task("Scanning BR files", total=len(br_files))
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        try:
            futures = {
                executor.submit(_scan_br_batch, batch, zip_output): batch
                for batch in batches
            }
            for future in as_completed(futures):
                all_results.extend(future.result())
                progress.advance(task, len(futures[future]))
        finally:
            executor.shutdown(wait=True)

    return all_results


# ── Phase 2: Build chain map (targeted file reads) ──


def _get_title(entity: dict) -> str:
    titles = entity.get(DCTERMS_TITLE, [])
    if titles:
        return titles[0].get("@value", "")
    return ""


def _get_types(entity: dict) -> frozenset[str]:
    return frozenset(entity.get("@type", []))


def _type_label(types: frozenset[str]) -> str:
    return ", ".join(
        sorted(t.rsplit("/", 1)[-1] for t in types if t != FABIO_EXPRESSION)
    )


def build_chain_map(
    container_uris: set[str],
    rdf_dir: str,
    dir_split: int,
    items_per_file: int,
    zip_output: bool,
) -> tuple[dict[str, list[str]], dict[str, tuple[str, frozenset[str]]]]:
    chain_map: dict[str, list[str]] = {}
    entity_meta: dict[str, tuple[str, frozenset[str]]] = {}
    needed = set(container_uris)
    depth = 0

    with create_progress() as progress:
        task = progress.add_task("Building chain map", total=len(needed))
        while needed:
            depth += 1
            file_to_uris: dict[str, set[str]] = defaultdict(set)
            for uri in needed:
                fpath = find_rdf_file(uri, rdf_dir, dir_split, items_per_file, zip_output)
                file_to_uris[fpath].add(uri)

            next_needed: set[str] = set()
            for fpath, uris in file_to_uris.items():
                if not os.path.exists(fpath):
                    progress.advance(task, len(uris))
                    continue
                for entity in _iter_entities([fpath], zip_output):
                    eid = entity["@id"]
                    if eid in uris:
                        entity_meta[eid] = (_get_title(entity), _get_types(entity))
                        if FRBR_PART_OF in entity:
                            parents = [p["@id"] for p in entity[FRBR_PART_OF]]
                            chain_map[eid] = parents
                            for p in parents:
                                if p not in chain_map and p not in needed:
                                    next_needed.add(p)
                progress.advance(task, len(uris))

            if next_needed:
                progress.update(
                    task,
                    description=f"Building chain map (depth {depth + 1})",
                    total=(progress.tasks[task].total or 0) + len(next_needed),
                )
            needed = next_needed

    return chain_map, entity_meta


# ── Phase 3: Classify & Resolve ──


def _follow_to_venue(uri: str, chain_map: dict[str, list[str]]) -> str:
    visited: set[str] = set()
    current = uri
    while current in chain_map:
        if current in visited:
            break
        visited.add(current)
        parents = chain_map[current]
        if len(parents) != 1:
            break
        current = parents[0]
    return current


def resolve_cases(
    raw_cases: list[tuple[str, list[str]]],
    chain_map: dict[str, list[str]],
    entity_meta: dict[str, tuple[str, frozenset[str]]],
) -> list[ResolvedCase]:
    resolved: list[ResolvedCase] = []
    for br_uri, container_uris in raw_cases:
        venues: list[str] = []
        for c_uri in container_uris:
            venues.append(_follow_to_venue(c_uri, chain_map))

        venue_set = set(venues)
        if len(venue_set) == 1:
            sorted_containers = sorted(container_uris)
            resolved.append(
                ResolvedCase(
                    br_uri=br_uri,
                    correct_part_of=sorted_containers[0],
                    to_remove=sorted_containers[1:],
                    method="same_venue",
                    reason=f"All chains converge to {venues[0]}",
                )
            )
            continue

        venue_keys: set[tuple[str, frozenset[str]]] = set()
        for v_uri in venue_set:
            if v_uri in entity_meta:
                title, types = entity_meta[v_uri]
                normalized = " ".join(title.strip().lower().split())
                venue_keys.add((normalized, types))
            else:
                venue_keys.add((v_uri, frozenset()))

        if len(venue_keys) == 1:
            sorted_containers = sorted(container_uris)
            resolved.append(
                ResolvedCase(
                    br_uri=br_uri,
                    correct_part_of=sorted_containers[0],
                    to_remove=sorted_containers[1:],
                    method="equivalent_venues",
                    reason=(
                        f"Chains reach different URIs "
                        f"({', '.join(sorted(venue_set))}) "
                        f"but same title/type"
                    ),
                )
            )
        else:
            venue_descs = []
            for v_uri in sorted(venue_set):
                if v_uri in entity_meta:
                    title, types = entity_meta[v_uri]
                    venue_descs.append(
                        f"'{title}' ({_type_label(types)})"
                    )
                else:
                    venue_descs.append(v_uri)
            resolved.append(
                ResolvedCase(
                    br_uri=br_uri,
                    correct_part_of=None,
                    to_remove=[],
                    method="manual_review",
                    reason=(
                        f"Chains reach different venues: "
                        f"{' vs '.join(venue_descs)}"
                    ),
                )
            )
    return resolved


# ── Enrich manual review cases ──



def _batch_read_entities(
    uris: set[str],
    rdf_dir: str,
    dir_split: int,
    items_per_file: int,
    zip_output: bool,
) -> dict[str, dict]:
    file_to_uris: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        file_to_uris[find_rdf_file(uri, rdf_dir, dir_split, items_per_file, zip_output)].add(uri)

    result: dict[str, dict] = {}
    for fpath, target_uris in file_to_uris.items():
        if not os.path.exists(fpath):
            continue
        for entity in _iter_entities([fpath], zip_output):
            if entity["@id"] in target_uris:
                result[entity["@id"]] = entity
    return result


def enrich_manual_review(
    manual_cases: list[ResolvedCase],
    raw_case_map: dict[str, list[str]],
    entity_meta: dict[str, tuple[str, frozenset[str]]],
    rdf_dir: str,
    dir_split: int,
    items_per_file: int,
    zip_output: bool,
) -> list[dict]:
    with create_progress() as progress:
        task = progress.add_task("Enriching manual cases", total=3)

        br_uris = {res.br_uri for res in manual_cases}
        br_entities = _batch_read_entities(
            br_uris, rdf_dir, dir_split, items_per_file, zip_output
        )
        progress.advance(task)

        id_uris: set[str] = set()
        for br_entity in br_entities.values():
            for id_ref in br_entity.get(HAS_IDENTIFIER, []):
                id_uris.add(id_ref["@id"])
        id_entities = _batch_read_entities(
            id_uris, rdf_dir, dir_split, items_per_file, zip_output
        )
        progress.advance(task)

        enriched = []
        for res in manual_cases:
            br_ids: list[str] = []
            br_entity = br_entities.get(res.br_uri)
            if br_entity:
                for id_ref in br_entity.get(HAS_IDENTIFIER, []):
                    id_entity = id_entities.get(id_ref["@id"])
                    if id_entity:
                        scheme_list = id_entity.get(USES_ID_SCHEME, [])
                        value_list = id_entity.get(HAS_LITERAL_VALUE, [])
                        if scheme_list and value_list:
                            scheme = scheme_list[0]["@id"].rsplit("/", 1)[-1]
                            value = value_list[0]["@value"]
                            br_ids.append(f"{scheme}:{value}")

            candidates = []
            for c_uri in raw_case_map[res.br_uri]:
                title, types = entity_meta.get(c_uri, ("", frozenset()))
                candidates.append(
                    {"uri": c_uri, "title": title, "type": _type_label(types)}
                )

            enriched.append(
                {
                    "br_uri": res.br_uri,
                    "identifiers": br_ids,
                    "candidates": candidates,
                    "reason": res.reason,
                }
            )
        progress.advance(task)
    return enriched


# ── Fix ──


def fix_br_part_of(
    editor: MetaEditor,
    br_uri: str,
    correct_part_of_uri: str,
    incorrect_part_of_uris: list[str],
) -> None:
    supplier_prefix = get_prefix(br_uri)
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )

    file_paths: set[str] = set()
    for uri in [br_uri, correct_part_of_uri] + incorrect_part_of_uris:
        file_paths.add(find_rdf_file(uri, editor.base_dir, editor.dir_split, editor.n_file_item, editor.zip_output_rdf))

    for fp in file_paths:
        imported = editor.reader.load(fp)
        if imported is not None:
            editor.reader.import_entities_from_graph(
                g_set, imported, editor.resp_agent
            )

    br_entity = g_set.get_entity(br_uri)
    correct_container = g_set.get_entity(correct_part_of_uri)
    assert br_entity is not None, f"BR not found: {br_uri}"
    assert correct_container is not None, f"Container not found: {correct_part_of_uri}"

    br_entity.remove_is_part_of()  # type: ignore[attr-defined]
    br_entity.is_part_of(correct_container)  # type: ignore[attr-defined]

    editor.save(g_set, supplier_prefix)


# ── Orphan check ──


def _check_orphan_batch(
    files: list[str], zip_output: bool, target_uris: list[str]
) -> set[str]:
    target_set = set(target_uris)
    referenced: set[str] = set()
    for entity in _iter_entities(files, zip_output):
        if FRBR_PART_OF in entity:
            for parent in entity[FRBR_PART_OF]:
                pid = parent["@id"]
                if pid in target_set:
                    referenced.add(pid)
    return referenced


def check_orphans(
    removed_uris: list[str],
    rdf_dir: str,
    zip_output: bool,
    workers: int = 4,
    batch_size: int = BATCH_SIZE,
) -> list[str]:
    if not removed_uris:
        return []

    removed_set = set(removed_uris)
    br_dir = os.path.join(rdf_dir, "br")
    if zip_output:
        br_files = collect_zip_files(br_dir, only_data=True)
    else:
        br_files = collect_files(br_dir, "*.json", lambda p: "prov" not in p)

    referenced: set[str] = set()
    batches = [
        br_files[i : i + batch_size] for i in range(0, len(br_files), batch_size)
    ]

    ctx = multiprocessing.get_context("forkserver")
    with create_progress() as progress:
        task = progress.add_task("Checking orphans", total=len(br_files))
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        try:
            futures = {
                executor.submit(
                    _check_orphan_batch, batch, zip_output, list(removed_set)
                ): batch
                for batch in batches
            }
            for future in as_completed(futures):
                referenced.update(future.result())
                progress.advance(task, len(futures[future]))
        finally:
            executor.shutdown(wait=True)

    return sorted(uri for uri in removed_uris if uri not in referenced)


# ── Progress ──


def _load_progress(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(json.load(f))


def _save_progress(path: str, completed: set[str]) -> None:
    with open(path, "w") as f:
        json.dump(sorted(completed), f)


# ── Report ──


def _build_report(
    resolved: list[ResolvedCase],
    manual_enriched: list[dict],
    orphans: list[str],
) -> dict:
    fixed = []
    for res in resolved:
        if res.correct_part_of is not None:
            fixed.append(
                {
                    "br_uri": res.br_uri,
                    "kept_part_of": res.correct_part_of,
                    "removed_part_of": res.to_remove,
                    "method": res.method,
                    "reason": res.reason,
                }
            )

    return {
        "summary": {
            "total_affected": len(resolved),
            "auto_fixed": len(fixed),
            "manual_review": len(manual_enriched),
            "orphaned_containers": len(orphans),
        },
        "fixed": fixed,
        "manual_review": manual_enriched,
        "orphaned_containers": orphans,
    }


# ── CLI ──


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Fix duplicate frbr:partOf values on bibliographic resources",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to meta_config.yaml"
    )
    parser.add_argument("-r", "--resp-agent", help="Responsible agent URI")
    parser.add_argument(
        "--dry-run", action="store_true", help="Report only, no modifications"
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=4, help="Parallel scan workers"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=BATCH_SIZE, help="Files per batch"
    )
    parser.add_argument(
        "--progress-file", default="fix_duplicate_part_of_progress.json"
    )
    parser.add_argument(
        "--report-file", default="fix_duplicate_part_of_report.json"
    )
    args = parser.parse_args()

    if not args.dry_run and not args.resp_agent:
        parser.error("--resp-agent is required when not using --dry-run")

    with open(args.config) as f:
        settings = yaml.safe_load(f)

    rdf_dir = os.path.join(settings["base_output_dir"], "rdf") + os.sep
    dir_split = settings["dir_split_number"]
    items_per_file = settings["items_per_file"]
    zip_output = settings["zip_output_rdf"]

    # Phase 1: Scan
    console.print("[bold]Scanning RDF files for duplicate partOf...[/bold]")
    raw_cases = scan_duplicate_part_of(
        rdf_dir, zip_output, args.workers, args.batch_size
    )
    console.print(f"Found {len(raw_cases)} BRs with duplicate partOf")

    if not raw_cases:
        console.print("[green]No duplicate partOf found.[/green]")
        return

    # Phase 2: Build chain map
    all_container_uris = {
        uri for _, containers in raw_cases for uri in containers
    }
    console.print(
        f"[bold]Building chain map for {len(all_container_uris)} containers...[/bold]"
    )
    chain_map, entity_meta = build_chain_map(
        all_container_uris, rdf_dir, dir_split, items_per_file, zip_output
    )

    # Phase 3: Classify & resolve
    resolved = resolve_cases(raw_cases, chain_map, entity_meta)
    auto_fix = [r for r in resolved if r.correct_part_of is not None]
    manual = [r for r in resolved if r.correct_part_of is None]
    console.print(f"Auto-fixable: {len(auto_fix)}, Manual review: {len(manual)}")

    # Enrich manual review cases for report
    raw_case_map = {br: containers for br, containers in raw_cases}
    manual_enriched = enrich_manual_review(
        manual, raw_case_map, entity_meta,
        rdf_dir, dir_split, items_per_file, zip_output,
    )

    if args.dry_run:
        report = _build_report(resolved, manual_enriched, [])
        with open(args.report_file, "w") as f:
            json.dump(report, f, indent=2)
        console.print(f"\n[bold]Dry run report written to {args.report_file}[/bold]")
        for res in manual:
            console.print(f"  [yellow]REVIEW[/yellow] {res.br_uri}: {res.reason}")
        return

    # Phase 4: Fix
    editor = MetaEditor(args.config, args.resp_agent)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    completed = _load_progress(args.progress_file)
    if completed:
        console.print(f"Resuming: {len(completed)} already processed")

    fixed_count = 0
    failed_count = 0

    with create_progress() as progress:
        task = progress.add_task("Fixing partOf", total=len(auto_fix))
        for res in auto_fix:
            if _stop_requested:
                console.print("[yellow]Interrupted, saving progress...[/yellow]")
                break
            if res.br_uri in completed:
                progress.advance(task)
                continue
            try:
                assert res.correct_part_of is not None
                fix_br_part_of(
                    editor, res.br_uri, res.correct_part_of, res.to_remove
                )
                completed.add(res.br_uri)
                _save_progress(args.progress_file, completed)
                fixed_count += 1
            except Exception as e:
                console.print(f"[red]Error fixing {res.br_uri}: {e}[/red]")
                failed_count += 1
            progress.advance(task)

    console.print(f"Fixed: {fixed_count}, Failed: {failed_count}")

    # Phase 5: Orphan check
    all_removed = [
        uri for res in resolved if res.correct_part_of for uri in res.to_remove
    ]
    console.print("[bold]Checking for orphaned containers...[/bold]")
    orphans = check_orphans(
        all_removed, rdf_dir, zip_output, args.workers, args.batch_size
    )
    console.print(f"Orphaned containers: {len(orphans)}")

    # Report
    report = _build_report(resolved, manual_enriched, orphans)
    with open(args.report_file, "w") as f:
        json.dump(report, f, indent=2)
    console.print(f"Report written to {args.report_file}")

    if not _stop_requested and failed_count == 0 and os.path.exists(args.progress_file):
        os.remove(args.progress_file)


if __name__ == "__main__":
    main()
