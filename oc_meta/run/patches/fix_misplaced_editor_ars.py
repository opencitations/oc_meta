# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import re
import signal
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from zipfile import ZipFile

import orjson
import yaml
from oc_ocdm.graph import GraphSet
from rich.progress import Progress, TaskID
from rich_argparse import RichHelpFormatter

from oc_meta.constants import CONTAINER_EDITOR_TYPES
from oc_meta.core.editor import MetaEditor
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import collect_files, collect_zip_files
from oc_meta.lib.finder import ResourceFinder

FRBR_PART_OF = "http://purl.org/vocab/frbr/core#partOf"
IS_DOC_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
WITH_ROLE = "http://purl.org/spar/pro/withRole"
EDITOR_ROLE = "http://purl.org/spar/pro/editor"
IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"
HAS_IDENTIFIER = "http://purl.org/spar/datacite/hasIdentifier"
USES_SCHEME = "http://purl.org/spar/datacite/usesIdentifierScheme"
HAS_LITERAL_VALUE = (
    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
)
FOAF_FAMILY_NAME = "http://xmlns.com/foaf/0.1/familyName"
FOAF_GIVEN_NAME = "http://xmlns.com/foaf/0.1/givenName"
FOAF_NAME = "http://xmlns.com/foaf/0.1/name"

CONTAINER_EDITOR_TYPE_IRIS = frozenset(
    iri for iri, label in ResourceFinder._IRI_TO_TYPE.items()
    if label in CONTAINER_EDITOR_TYPES
)

_URI_RE = re.compile(r"^.+/([a-z]{2})/(0[1-9]+0)([1-9][0-9]*)$")

BATCH_SIZE = 100

_stop_requested = False


def _worker_init() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def _handle_signal(_signum: int, _frame: object) -> None:
    global _stop_requested
    _stop_requested = True
    console.print("[yellow]Interrupt received, finishing current entity...[/yellow]")


def _get_supplier_prefix(uri: str) -> str:
    match = re.match(r"^(.+)/([a-z][a-z])/(0[1-9]+0)([1-9][0-9]*)$", uri)
    assert match is not None, f"Cannot extract supplier prefix from: {uri}"
    return match.group(3)


def _uri_to_file_path(
    rdf_dir: str, uri: str, dir_split: int, items_per_file: int, zip_output: bool
) -> str:
    m = _URI_RE.match(uri)
    assert m, f"Cannot parse URI: {uri}"
    entity_type, prefix = m.group(1), m.group(2)
    number = int(m.group(3))
    cur_split = math.ceil(number / dir_split) * dir_split
    cur_file = math.ceil(number / items_per_file) * items_per_file
    ext = ".zip" if zip_output else ".json"
    return os.path.join(rdf_dir, entity_type, prefix, str(cur_split), str(cur_file) + ext)


def _group_by_file(
    uris: set[str], rdf_dir: str, dir_split: int, items_per_file: int, zip_output: bool
) -> dict[str, set[str]]:
    file_to_uris: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        fpath = _uri_to_file_path(rdf_dir, uri, dir_split, items_per_file, zip_output)
        file_to_uris[fpath].add(uri)
    return dict(file_to_uris)


def _make_targeted_batches(
    file_targets: dict[str, set[str]], batch_size: int
) -> list[list[tuple[str, frozenset[str]]]]:
    items = [(fpath, frozenset(uris)) for fpath, uris in file_targets.items()]
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


def _read_file(fpath: str, zip_output: bool) -> list:
    if zip_output:
        with ZipFile(fpath, "r") as zf:
            return orjson.loads(zf.read(zf.namelist()[0]))
    with open(fpath, "rb") as f:
        return orjson.loads(f.read())


def _iter_entities(files: list, zip_output: bool):
    for fpath in files:
        for graph in _read_file(fpath, zip_output):
            for entity in graph.get("@graph", []):
                yield entity


def _scan_br_content_batch(
    files: list[str], zip_output: bool
) -> tuple[dict[str, str], dict[str, set[str]], list[str]]:
    frbr_part_of: dict[str, str] = {}
    content_ars: dict[str, set[str]] = {}
    warnings: list[str] = []
    for entity in _iter_entities(files, zip_output):
        eid = entity["@id"]
        entity_types = set(entity.get("@type", []))
        if entity_types & CONTAINER_EDITOR_TYPE_IRIS and FRBR_PART_OF in entity:
            parents = entity[FRBR_PART_OF]
            if len(parents) > 1:
                parent_ids = [p["@id"] for p in parents]
                warnings.append(
                    f"{eid} has {len(parents)} frbr:partOf values {parent_ids}; using first only"
                )
            frbr_part_of[eid] = parents[0]["@id"]
            if IS_DOC_CONTEXT_FOR in entity:
                content_ars[eid] = {x["@id"] for x in entity[IS_DOC_CONTEXT_FOR]}
    return frbr_part_of, content_ars, warnings


def _scan_ar_editors_batch(
    files: list[str], zip_output: bool
) -> dict[str, str]:
    editors: dict[str, str] = {}
    for entity in _iter_entities(files, zip_output):
        if WITH_ROLE in entity and entity[WITH_ROLE][0]["@id"] == EDITOR_ROLE:
            editors[entity["@id"]] = entity[IS_HELD_BY][0]["@id"]
    return editors


def _scan_container_ars_batch(
    file_targets: list[tuple[str, frozenset[str]]], zip_output: bool
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for fpath, targets in file_targets:
        for graph in _read_file(fpath, zip_output):
            for entity in graph.get("@graph", []):
                eid = entity["@id"]
                if eid in targets and IS_DOC_CONTEXT_FOR in entity:
                    result[eid] = {x["@id"] for x in entity[IS_DOC_CONTEXT_FOR]}
    return result


def _scan_ra_info_batch(
    file_targets: list[tuple[str, frozenset[str]]], zip_output: bool
) -> tuple[dict[str, set[str]], dict[str, str]]:
    id_result: dict[str, set[str]] = {}
    name_result: dict[str, str] = {}
    for fpath, targets in file_targets:
        for graph in _read_file(fpath, zip_output):
            for entity in graph.get("@graph", []):
                eid = entity["@id"]
                if eid not in targets:
                    continue
                if HAS_IDENTIFIER in entity:
                    id_result[eid] = {x["@id"] for x in entity[HAS_IDENTIFIER]}
                family = entity.get(FOAF_FAMILY_NAME, [{}])[0].get("@value", "")
                given = entity.get(FOAF_GIVEN_NAME, [{}])[0].get("@value", "")
                if family:
                    name = f"{family.lower()}, {given.lower()}" if given else family.lower()
                else:
                    full = entity.get(FOAF_NAME, [{}])[0].get("@value", "")
                    name = full.lower() if full else ""
                if name:
                    name_result[eid] = name
    return id_result, name_result


def _scan_id_values_batch(
    file_targets: list[tuple[str, frozenset[str]]], zip_output: bool
) -> dict[str, str]:
    result: dict[str, str] = {}
    for fpath, targets in file_targets:
        for graph in _read_file(fpath, zip_output):
            for entity in graph.get("@graph", []):
                eid = entity["@id"]
                if eid in targets and USES_SCHEME in entity:
                    scheme_iri = entity[USES_SCHEME][0]["@id"]
                    scheme_name = scheme_iri.rsplit("/", 1)[-1]
                    value = entity[HAS_LITERAL_VALUE][0]["@value"]
                    result[eid] = f"{scheme_name}:{value}"
    return result


def _run_parallel(
    executor: ProcessPoolExecutor,
    scan_fn: Callable,
    batches: list,
    zip_output: bool,
    progress: Progress,
    task_id: TaskID,
) -> list:
    futures = {
        executor.submit(scan_fn, batch, zip_output): batch
        for batch in batches
    }
    results = []
    try:
        for future in as_completed(futures):
            results.append(future.result())
            progress.advance(task_id, len(futures[future]))
    except KeyboardInterrupt:
        for f in futures:
            f.cancel()
        raise
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return results


def _classify_actions(
    container_to_contents: dict[str, list[str]],
    content_editor_ars: dict[str, set[str]],
    editor_ar_to_ra: dict[str, str],
    ra_identifiers: dict[str, set[str]],
    ra_names: dict[str, str],
    container_editor_ars: dict[str, set[str]],
) -> list[dict]:
    results: list[dict] = []
    for container, contents in container_to_contents.items():
        known_ras: set[str] = set()
        known_ids: set[str] = set()
        known_names: set[str] = set()
        for ar in container_editor_ars.get(container, set()):
            ra = editor_ar_to_ra[ar]
            known_ras.add(ra)
            known_ids.update(ra_identifiers.get(ra, set()))
            name = ra_names.get(ra, "")
            if name:
                known_names.add(name)

        for content in sorted(contents):
            for ar in sorted(content_editor_ars[content]):
                ra = editor_ar_to_ra[ar]
                ids = ra_identifiers.get(ra, set())
                name = ra_names.get(ra, "")

                if ra in known_ras:
                    action = "skip_duplicate_ra"
                    match_reason = ra
                elif ids & known_ids:
                    action = "skip_duplicate_id"
                    match_reason = next(iter(ids & known_ids))
                elif name and name in known_names:
                    action = "skip_duplicate_name"
                    match_reason = name
                else:
                    action = "move"
                    match_reason = None
                    known_ras.add(ra)
                    known_ids.update(ids)
                    if name:
                        known_names.add(name)

                results.append({
                    "content": content,
                    "container": container,
                    "ar": ar,
                    "ra": ra,
                    "identifiers": sorted(ids),
                    "action": action,
                    "match_reason": match_reason,
                })
    return results


def find_misplaced_editor_ars(
    base_dir: str,
    zip_output: bool,
    dir_split: int,
    items_per_file: int,
    workers: int = 4,
    batch_size: int = BATCH_SIZE,
) -> list[dict]:
    br_dir = os.path.join(base_dir, "br")
    ar_dir = os.path.join(base_dir, "ar")

    if zip_output:
        br_files = collect_zip_files(br_dir, only_data=True)
        ar_files = collect_zip_files(ar_dir, only_data=True)
    else:
        br_files = collect_files(br_dir, "*.json", lambda p: "prov" not in p)
        ar_files = collect_files(ar_dir, "*.json", lambda p: "prov" not in p)

    br_batches = [br_files[i:i + batch_size] for i in range(0, len(br_files), batch_size)]
    ar_batches = [ar_files[i:i + batch_size] for i in range(0, len(ar_files), batch_size)]

    frbr_part_of: dict[str, str] = {}
    content_ars: dict[str, set[str]] = {}

    ctx = multiprocessing.get_context("forkserver")

    with create_progress() as progress:
        # Phase 1: BR scan
        br_task = progress.add_task("Scanning BR files", total=len(br_files))
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        for partial_frbr, partial_content_ars, warnings in _run_parallel(
            executor, _scan_br_content_batch, br_batches, zip_output, progress, br_task
        ):
            frbr_part_of.update(partial_frbr)
            content_ars.update(partial_content_ars)
            for w in warnings:
                console.print(f"[yellow]Warning:[/yellow] {w}")

        console.print(
            f"BR scan complete: [cyan]{len(frbr_part_of)}[/cyan] content entities, "
            f"[cyan]{sum(len(v) for v in content_ars.values())}[/cyan] content ARs"
        )

        # Phase 2: AR scan (returns ar→ra mapping)
        ar_task = progress.add_task("Scanning AR files", total=len(ar_files))
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        editor_ar_to_ra: dict[str, str] = {}
        for partial in _run_parallel(
            executor, _scan_ar_editors_batch, ar_batches, zip_output, progress, ar_task
        ):
            editor_ar_to_ra.update(partial)

        console.print(
            f"AR scan complete: [cyan]{len(editor_ar_to_ra)}[/cyan] editor ARs found"
        )

    # Identify misplaced editor ARs per content entity
    content_editor_ars: dict[str, set[str]] = {}
    for content, ars in content_ars.items():
        editors = ars & editor_ar_to_ra.keys()
        if editors:
            content_editor_ars[content] = editors

    container_to_contents: dict[str, list[str]] = defaultdict(list)
    for content in content_editor_ars:
        container_to_contents[frbr_part_of[content]].append(content)

    container_uris = set(container_to_contents.keys())
    console.print(
        f"Identified [cyan]{sum(len(v) for v in content_editor_ars.values())}[/cyan] "
        f"misplaced editor ARs across [cyan]{len(content_editor_ars)}[/cyan] content "
        f"entities in [cyan]{len(container_uris)}[/cyan] containers"
    )

    # Phase 3: targeted container AR scan
    container_file_targets = _group_by_file(
        container_uris, base_dir, dir_split, items_per_file, zip_output
    )
    container_batches = _make_targeted_batches(container_file_targets, batch_size)

    container_ars: dict[str, set[str]] = {}
    container_editor_ars: dict[str, set[str]] = {}

    with create_progress() as progress:
        ct_task = progress.add_task(
            "Scanning container ARs", total=len(container_file_targets)
        )
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        for partial in _run_parallel(
            executor, _scan_container_ars_batch, container_batches,
            zip_output, progress, ct_task
        ):
            container_ars.update(partial)

    for container, ars in container_ars.items():
        editors = ars & editor_ar_to_ra.keys()
        if editors:
            container_editor_ars[container] = editors

    console.print(
        f"Container scan: [cyan]{len(container_editor_ars)}[/cyan] containers "
        f"already have editor ARs"
    )

    # Phase 4: determine which containers need dedup and collect RA identifiers
    containers_needing_dedup: set[str] = set()
    for container, contents in container_to_contents.items():
        if (
            container in container_editor_ars
            or len(contents) > 1
            or any(len(content_editor_ars[c]) > 1 for c in contents)
        ):
            containers_needing_dedup.add(container)

    ras_needing_ids: set[str] = set()
    for container in containers_needing_dedup:
        for ar in container_editor_ars.get(container, set()):
            ras_needing_ids.add(editor_ar_to_ra[ar])
        for content in container_to_contents[container]:
            for ar in content_editor_ars[content]:
                ras_needing_ids.add(editor_ar_to_ra[ar])

    ra_identifiers: dict[str, set[str]] = {}
    ra_names: dict[str, str] = {}

    if ras_needing_ids:
        console.print(
            f"Collecting info for [cyan]{len(ras_needing_ids)}[/cyan] RAs "
            f"across [cyan]{len(containers_needing_dedup)}[/cyan] containers needing dedup"
        )
        ra_file_targets = _group_by_file(
            ras_needing_ids, base_dir, dir_split, items_per_file, zip_output
        )
        ra_batches = _make_targeted_batches(ra_file_targets, batch_size)

        ra_to_id_uris: dict[str, set[str]] = {}
        with create_progress() as progress:
            ra_task = progress.add_task(
                "Collecting RA info", total=len(ra_file_targets)
            )
            executor = ProcessPoolExecutor(
                max_workers=workers, initializer=_worker_init, mp_context=ctx
            )
            for partial_ids, partial_names in _run_parallel(
                executor, _scan_ra_info_batch, ra_batches,
                zip_output, progress, ra_task
            ):
                ra_to_id_uris.update(partial_ids)
                ra_names.update(partial_names)

        all_id_uris: set[str] = set()
        for ids in ra_to_id_uris.values():
            all_id_uris.update(ids)

        if all_id_uris:
            id_file_targets = _group_by_file(
                all_id_uris, base_dir, dir_split, items_per_file, zip_output
            )
            id_batches = _make_targeted_batches(id_file_targets, batch_size)

            id_to_value: dict[str, str] = {}
            with create_progress() as progress:
                id_task = progress.add_task(
                    "Collecting ID values", total=len(id_file_targets)
                )
                executor = ProcessPoolExecutor(
                    max_workers=workers, initializer=_worker_init, mp_context=ctx
                )
                for partial in _run_parallel(
                    executor, _scan_id_values_batch, id_batches,
                    zip_output, progress, id_task
                ):
                    id_to_value.update(partial)

            for ra, id_uris in ra_to_id_uris.items():
                ids = {id_to_value[id_uri] for id_uri in id_uris if id_uri in id_to_value}
                if ids:
                    ra_identifiers[ra] = ids

        console.print(
            f"RA info complete: [cyan]{len(ra_identifiers)}[/cyan] with identifiers, "
            f"[cyan]{len(ra_names)}[/cyan] with names"
        )

    return _classify_actions(
        dict(container_to_contents), content_editor_ars,
        editor_ar_to_ra, ra_identifiers, ra_names, container_editor_ars,
    )


def fix_content(
    editor: MetaEditor,
    content_uri: str,
    container_uri: str,
    move_ar_uris: list[str],
    skip_ar_uris: list[str],
) -> None:
    supplier_prefix = _get_supplier_prefix(content_uri)
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )

    file_paths: set[str] = set()
    all_ar_uris = move_ar_uris + skip_ar_uris
    for uri in [content_uri, container_uri] + all_ar_uris:
        fp = editor.find_file(
            editor.base_dir, editor.dir_split, editor.n_file_item, uri, editor.zip_output_rdf
        )
        if fp is not None:
            file_paths.add(fp)

    for fp in file_paths:
        imported_graph = editor.reader.load(fp)
        if imported_graph is not None:
            editor.reader.import_entities_from_graph(g_set, imported_graph, editor.resp_agent)

    content_entity = g_set.get_entity(content_uri)
    container_entity = g_set.get_entity(container_uri)
    assert content_entity is not None, f"content not found: {content_uri}"
    assert container_entity is not None, f"container not found: {container_uri}"

    for ar_uri in all_ar_uris:
        ar_entity = g_set.get_entity(ar_uri)
        assert ar_entity is not None, f"AR not found: {ar_uri}"
        content_entity.remove_contributor(ar_entity)  # type: ignore[attr-defined]
        ar_entity.remove_next()  # type: ignore[attr-defined]

    if move_ar_uris:
        move_entities = []
        for ar_uri in move_ar_uris:
            ar_entity = g_set.get_entity(ar_uri)
            container_entity.has_contributor(ar_entity)  # type: ignore[attr-defined]
            move_entities.append(ar_entity)

        for i in range(len(move_entities) - 1):
            move_entities[i].has_next(move_entities[i + 1])  # type: ignore[attr-defined]

    editor.save(g_set, supplier_prefix)


def _load_progress(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(json.load(f))


def _save_progress(path: str, completed: set[str]) -> None:
    with open(path, "w") as f:
        json.dump(list(completed), f)


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Fix misplaced editor ARs: move pro:isDocumentContextFor "
            "from content entity to its frbr:partOf container"
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Path to meta_config.yaml")
    parser.add_argument("-r", "--resp-agent", help="Responsible agent URI (required without --dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Report cases without modifying")
    parser.add_argument(
        "-w", "--workers", type=int, default=4, help="Number of parallel workers for scanning"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=BATCH_SIZE, help="Files per batch for scanning"
    )
    parser.add_argument(
        "--progress-file",
        default="fix_misplaced_editor_ars_progress.json",
        help=(
            "Path to a JSON file used to track completed content entities for resumable execution. "
            "Created automatically if it does not exist; deleted on successful completion. "
            "Default: fix_misplaced_editor_ars_progress.json in the current working directory."
        ),
    )
    parser.add_argument(
        "--report-file",
        default="fix_misplaced_editor_ars_report.json",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.resp_agent:
        parser.error("--resp-agent is required when not using --dry-run")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    rdf_dir = os.path.join(config["base_output_dir"], "rdf")
    zip_output = config.get("zip_output_rdf", False)
    dir_split = config["dir_split_number"]
    items_per_file = config["items_per_file"]

    console.print("Scanning RDF files for misplaced editor ARs...")
    cases = find_misplaced_editor_ars(
        rdf_dir, zip_output, dir_split, items_per_file,
        args.workers, args.batch_size,
    )

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for case in cases:
        groups[(case["content"], case["container"])].append(case)

    total_ars = len(cases)
    move_count = sum(1 for c in cases if c["action"] == "move")
    skip_ra_count = sum(1 for c in cases if c["action"] == "skip_duplicate_ra")
    skip_id_count = sum(1 for c in cases if c["action"] == "skip_duplicate_id")
    skip_name_count = sum(1 for c in cases if c["action"] == "skip_duplicate_name")

    console.print(
        f"\n[bold]Found [green]{total_ars}[/green] misplaced editor ARs "
        f"across [green]{len(groups)}[/green] content entities[/bold]\n"
        f"  [green]{move_count}[/green] to move, "
        f"[yellow]{skip_ra_count}[/yellow] skip (same RA), "
        f"[yellow]{skip_id_count}[/yellow] skip (same identifier), "
        f"[yellow]{skip_name_count}[/yellow] skip (same name)"
    )

    if args.dry_run:
        unique_containers = {c["container"] for c in cases}
        report = {
            "summary": {
                "total_misplaced_ars": total_ars,
                "affected_content_entities": len(groups),
                "unique_containers": len(unique_containers),
                "ars_to_move": move_count,
                "ars_skipped_duplicate_ra": skip_ra_count,
                "ars_skipped_duplicate_id": skip_id_count,
                "ars_skipped_duplicate_name": skip_name_count,
            },
            "cases": [
                {
                    "content": content,
                    "container": container,
                    "editor_ars": [
                        {
                            "ar": a["ar"],
                            "ra": a["ra"],
                            "identifiers": a["identifiers"],
                            "action": a["action"],
                            "match_reason": a["match_reason"],
                        }
                        for a in actions
                    ],
                }
                for (content, container), actions in sorted(groups.items())
            ],
        }
        with open(args.report_file, "w") as f:
            json.dump(report, f, indent=2)
        console.print(f"\n[bold]Dry run report written to {args.report_file}[/bold]")
        return

    completed = _load_progress(args.progress_file)
    if completed:
        console.print(f"Resuming: {len(completed)} content entities already processed")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    editor = MetaEditor(args.config, args.resp_agent)
    succeeded = failed = skipped = 0

    with create_progress() as progress:
        task = progress.add_task("Fixing misplaced editor ARs", total=len(groups))
        for (content, container), ar_actions in groups.items():
            if _stop_requested:
                break
            if content in completed:
                skipped += 1
                progress.advance(task)
                continue
            try:
                move_ars = [a["ar"] for a in ar_actions if a["action"] == "move"]
                skip_ars = [a["ar"] for a in ar_actions if a["action"].startswith("skip")]
                fix_content(editor, content, container, move_ars, skip_ars)
                completed.add(content)
                _save_progress(args.progress_file, completed)
                succeeded += 1
            except Exception as e:
                console.print(f"  [red]Error[/red] {content.split('/')[-1]}: {e}")
                failed += 1
            progress.advance(task)

    if _stop_requested:
        console.print(f"Stopped: {succeeded} fixed, {failed} failed, {skipped} skipped")
    else:
        console.print(f"Done: {succeeded} fixed, {failed} failed, {skipped} skipped")
        if os.path.exists(args.progress_file) and not failed:
            os.remove(args.progress_file)


if __name__ == "__main__":
    main()
