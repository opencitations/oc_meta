# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import os
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import cast

import numpy as np
import numpy.typing as npt
import yaml
from oc_ocdm.graph import GraphSet
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.agent_metadata import AgentMetadataClient, ApiCache
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.rdf_patch import (
    HAS_IDENTIFIER,
    HAS_NEXT,
    IS_DOCUMENT_CONTEXT_FOR,
    ROLE_MAP,
    WITH_ROLE,
    EntityFileLocator,
    agent_role,
    batches,
    data_files,
    ensure_parent,
    ids,
    import_exact_entities,
    load_audit_config,
    load_available_entities,
    load_entities,
    mp_method,
    provenance_path,
    recycled_map,
    sha256,
    snapshot_number,
    write_json,
)
from oc_meta.run.patches import plan_execution
from oc_meta.run.patches.fix_appended_roles import (
    CONTEXT_TRIPLE,
    check_work_preconditions,
    choose_record,
    doi_order,
    entity_hashes,
    load_role_summaries,
    missing_role_types,
    original_identifiers,
    remove_work_roles,
    triple_changes,
    updates_newest_first,
    work_roles,
)
from oc_meta.run.patches.fix_dangling_ars import (
    PROVIDERS,
    verify_csv_files,
    work_csv_row,
    write_provider_csvs,
)
from oc_meta.run.patches.plan_execution import identified, object_sha256

PLAN_VERSION = 2

RoleNumbers = npt.NDArray[np.int64]


def _role_number(uri: str) -> int:
    # The leading 1 keeps the zeros at the start of the OMID suffix.
    return int("1" + uri.rsplit("/", 1)[1])


def _role_uri(base_iri: str, number: int) -> str:
    return f"{base_iri}ar/{str(number)[1:]}"


def _referenced_roles(paths: list[str]) -> RoleNumbers:
    return np.array(
        [
            _role_number(role)
            for path in paths
            for entity in load_entities(path).values()
            for role in ids(entity, IS_DOCUMENT_CONTEXT_FOR)
        ],
        dtype=np.int64,
    )


def _role_links(paths: list[str]) -> tuple[RoleNumbers, RoleNumbers, RoleNumbers]:
    roles = []
    sources = []
    targets = []
    for path in paths:
        for uri, entity in load_entities(path).items():
            role = _role_number(uri)
            roles.append(role)
            for next_role in ids(entity, HAS_NEXT):
                sources.append(role)
                targets.append(_role_number(next_role))
    return (
        np.array(roles, dtype=np.int64),
        np.array(sources, dtype=np.int64),
        np.array(targets, dtype=np.int64),
    )


def _new_pool(workers: int) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(max_workers=workers, mp_context=mp_method)


def _scan_referenced_roles(rdf_dir: str, zip_output: bool, workers: int) -> RoleNumbers:
    files = data_files(os.path.join(rdf_dir, "br"), zip_output)
    file_batches = batches(files, 24)
    parts = []
    with create_progress() as progress:
        task = progress.add_task("Reading bibliographic resources", total=len(files))
        for paths, part in zip(
            file_batches,
            recycled_map(
                _referenced_roles, file_batches, workers, lambda: _new_pool(workers)
            ),
        ):
            parts.append(part)
            progress.advance(task, len(paths))
    return np.unique(np.concatenate(parts))


def _scan_role_links(
    rdf_dir: str, zip_output: bool, workers: int
) -> tuple[RoleNumbers, RoleNumbers, RoleNumbers]:
    files = data_files(os.path.join(rdf_dir, "ar"), zip_output)
    file_batches = batches(files, 24)
    roles = []
    sources = []
    targets = []
    with create_progress() as progress:
        task = progress.add_task("Reading agent roles", total=len(files))
        for paths, (part_roles, part_sources, part_targets) in zip(
            file_batches,
            recycled_map(
                _role_links, file_batches, workers, lambda: _new_pool(workers)
            ),
        ):
            roles.append(part_roles)
            sources.append(part_sources)
            targets.append(part_targets)
            progress.advance(task, len(paths))
    return np.concatenate(roles), np.concatenate(sources), np.concatenate(targets)


_orphans: frozenset[str] = frozenset()


def _init_lost_link_scan(orphans: frozenset[str]) -> None:
    global _orphans
    _orphans = orphans


def _raw_text(path: str) -> str:
    if path.endswith(".zip"):
        with zipfile.ZipFile(path) as archive:
            return "".join(
                archive.read(name).decode("utf-8") for name in archive.namelist()
            )
    with open(path, encoding="utf-8") as stream:
        return stream.read()


def linked_by_provenance(snapshots: list[tuple[int, dict[str, object]]]) -> set[str]:
    linked: set[str] = set()
    for _, _, query in reversed(updates_newest_first(snapshots)):
        inserted, deleted = triple_changes(query, CONTEXT_TRIPLE)
        linked = (linked - deleted) | inserted
    return linked


def _lost_links_in_file(path: str, zip_output: bool) -> list[dict[str, object]]:
    prov_path = provenance_path(path, zip_output)
    if not os.path.exists(prov_path):
        return []
    mentioned = {
        match.group(1) for match in CONTEXT_TRIPLE.finditer(_raw_text(prov_path))
    }
    if not mentioned & _orphans:
        return []
    works = load_entities(path)
    snapshots: dict[str, list[tuple[int, dict[str, object]]]] = defaultdict(list)
    for uri, entity in load_entities(prov_path).items():
        work = uri.split("/prov/se/")[0]
        if work in works:
            snapshots[work].append((snapshot_number(uri), entity))
    records = []
    for work, history in snapshots.items():
        entity = works[work]
        lost = (
            linked_by_provenance(history) - set(ids(entity, IS_DOCUMENT_CONTEXT_FOR))
        ) & _orphans
        if lost:
            identifiers = set(ids(entity, HAS_IDENTIFIER))
            records.append(
                {
                    "br": work,
                    "lost_roles": sorted(lost),
                    "roles": ids(entity, IS_DOCUMENT_CONTEXT_FOR),
                    "identifiers": sorted(identifiers),
                    "original_identifiers": sorted(
                        original_identifiers(identifiers, history)
                    ),
                }
            )
    return records


def _lost_links_batch(task: tuple[list[str], bool]) -> list[dict[str, object]]:
    paths, zip_output = task
    return [
        record for path in paths for record in _lost_links_in_file(path, zip_output)
    ]


def find_lost_links(
    rdf_dir: str, zip_output: bool, orphan_uris: set[str], workers: int
) -> list[dict[str, object]]:
    files = data_files(os.path.join(rdf_dir, "br"), zip_output)
    tasks = [(batch, zip_output) for batch in batches(files, 8)]
    records = []
    with create_progress() as progress:
        task = progress.add_task("Reading BR provenance", total=len(files))
        for (paths, _), partial in zip(
            tasks,
            recycled_map(
                _lost_links_batch,
                tasks,
                workers,
                lambda: ProcessPoolExecutor(
                    max_workers=workers,
                    mp_context=mp_method,
                    initializer=_init_lost_link_scan,
                    initargs=(frozenset(orphan_uris),),
                ),
            ),
        ):
            records.extend(partial)
            progress.advance(task, len(paths))
    return records


def _reingestion_blockers(
    work: dict[str, object],
    orphan_entities: dict[str, dict[str, object]],
) -> list[str]:
    if any(
        len(ids(orphan_entities[role], WITH_ROLE)) != 1
        or ids(orphan_entities[role], WITH_ROLE)[0] not in ROLE_MAP
        for role in cast(list[str], work["lost_roles"])
    ):
        return ["malformed_role"]
    return []


def _external_links(
    removed: dict[str, str],
    orphans: RoleNumbers,
    sources: RoleNumbers,
    targets: RoleNumbers,
    base_iri: str,
) -> set[str]:
    numbers = np.array([_role_number(uri) for uri in removed], dtype=np.int64)
    touching = np.isin(sources, numbers) | np.isin(targets, numbers)
    blocked = set()
    for source, target in zip(sources[touching], targets[touching]):
        ends = [_role_uri(base_iri, int(number)) for number in (source, target)]
        owners = {removed[uri] for uri in ends if uri in removed}
        outside = [
            number
            for number, uri in zip((source, target), ends)
            if uri not in removed and not np.isin(number, orphans)
        ]
        if len(owners) > 1 or outside:
            blocked.update(owners)
    return blocked


def _plan_reingestion(
    lost: list[dict[str, object]],
    orphan_entities: dict[str, dict[str, object]],
    links: tuple[RoleNumbers, RoleNumbers, RoleNumbers],
    base_iri: str,
    locator: EntityFileLocator,
    cache_path: str,
    mailto: str,
    workers: int,
    refresh_cache: bool,
) -> tuple[
    list[dict[str, object]], list[dict[str, object]], dict[str, list[dict[str, str]]]
]:
    orphans, sources, targets = links
    blocked: list[dict[str, object]] = []
    works = []
    for work in lost:
        blockers = _reingestion_blockers(work, orphan_entities)
        if blockers:
            blocked.append(
                {
                    "br": work["br"],
                    "blockers": blockers,
                    "lost_roles": work["lost_roles"],
                }
            )
        else:
            works.append(work)
    summaries = load_role_summaries(
        {role for work in works for role in cast(list[str], work["roles"])},
        locator,
        workers,
    )
    for work in works:
        lost_types = {
            ROLE_MAP[ids(orphan_entities[role], WITH_ROLE)[0]]
            for role in cast(list[str], work["lost_roles"])
        }
        work["role_types"] = {
            role_type: sorted(
                role
                for role in cast(list[str], work["roles"])
                if ROLE_MAP[summaries[role].role_types[0]] == role_type
            )
            for role_type in sorted(lost_types)
        }
    removed = {
        role: cast(str, work["br"]) for work in works for role in work_roles(work)
    }
    linked_outside = _external_links(removed, orphans, sources, targets, base_iri)
    blocked.extend(
        {
            "br": work["br"],
            "blockers": ["external_has_next"],
            "lost_roles": work["lost_roles"],
        }
        for work in works
        if work["br"] in linked_outside
    )
    works = [work for work in works if work["br"] not in linked_outside]
    entities = load_available_entities(
        {
            uri
            for work in works
            for uri in {
                cast(str, work["br"]),
                *work_roles(work),
                *cast(list[str], work["identifiers"]),
            }
        },
        locator,
        workers,
    )

    ensure_parent(cache_path)
    api_cache = ApiCache(cache_path)
    provider = AgentMetadataClient(
        mailto=mailto, cache=api_cache, refresh_cache=refresh_cache
    )
    operations = []
    rows_by_provider: dict[str, list[dict[str, str]]] = {name: [] for name in PROVIDERS}
    try:
        with create_progress() as progress:
            task = progress.add_task("Querying work metadata", total=len(works))
            for work in works:
                work_uri = cast(str, work["br"])
                role_types = cast(dict[str, list[str]], work["role_types"])
                dois = doi_order(work, entities)
                doi, selected, metadata, attempts = choose_record(provider, dois)
                progress.advance(task)
                if metadata is None or selected is None:
                    blocked.append(
                        {
                            "br": work_uri,
                            "blockers": [
                                "no_doi" if not dois else "provider_unavailable"
                            ],
                            "attempts": attempts,
                            "lost_roles": work["lost_roles"],
                        }
                    )
                    continue
                missing = missing_role_types(metadata, sorted(role_types))
                if missing:
                    blocked.append(
                        {
                            "br": work_uri,
                            "blockers": ["provider_missing_role_type"],
                            "doi": doi,
                            "provider": selected,
                            "missing_role_types": missing,
                            "lost_roles": work["lost_roles"],
                        }
                    )
                    continue
                operation: dict[str, object] = {
                    "br": work_uri,
                    "doi": doi,
                    "provider": selected,
                    "role_types": role_types,
                    "lost_roles": work["lost_roles"],
                }
                operation["entities_sha256"] = entity_hashes(operation, entities)
                operations.append(identified(operation))
                rows_by_provider[selected].append(
                    work_csv_row(
                        work_uri, entities[work_uri], doi, metadata, sorted(role_types)
                    )
                )
    finally:
        provider.close()
        api_cache.close()
    operations.sort(key=lambda operation: cast(str, operation["br"]))
    blocked.sort(key=lambda entry: cast(str, entry["br"]))
    return operations, blocked, rows_by_provider


def analyze_orphan_roles(
    config_path: str,
    report_path: str,
    csv_output_dir: str,
    cache_path: str,
    mailto: str,
    workers: int,
    refresh_cache: bool,
) -> dict[str, object]:
    config_path = os.path.abspath(config_path)
    csv_output_dir = os.path.abspath(csv_output_dir)
    cache_path = os.path.abspath(cache_path)
    config = load_audit_config(config_path)
    with open(config_path, encoding="utf-8") as stream:
        base_iri = cast(str, yaml.safe_load(stream)["base_iri"])
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    referenced = _scan_referenced_roles(config.rdf_dir, config.zip_output, workers)
    roles, sources, targets = _scan_role_links(
        config.rdf_dir, config.zip_output, workers
    )
    orphans = np.unique(roles[~np.isin(roles, referenced)])
    orphan_sources = np.isin(sources, orphans)
    orphan_targets = np.isin(targets, orphans)
    incoming = ~orphan_sources & orphan_targets
    if incoming.any():
        examples = [
            f"{_role_uri(base_iri, int(source))} -> {_role_uri(base_iri, int(target))}"
            for source, target in zip(sources[incoming][:10], targets[incoming][:10])
        ]
        raise RuntimeError(
            "Referenced agent roles link to orphan roles: " + "; ".join(examples)
        )

    orphan_uris = {_role_uri(base_iri, int(number)) for number in orphans}
    entities = load_available_entities(orphan_uris, locator, workers)
    lost = find_lost_links(config.rdf_dir, config.zip_output, orphan_uris, workers)
    reingestion, blocked, rows_by_provider = _plan_reingestion(
        lost,
        entities,
        (orphans, sources, targets),
        base_iri,
        locator,
        cache_path,
        mailto,
        workers,
        refresh_cache,
    )
    kept = {role for entry in blocked for role in cast(list[str], entry["lost_roles"])}
    roles_by_path: dict[str, dict[str, str]] = defaultdict(dict)
    for uri in sorted(orphan_uris - kept):
        roles_by_path[locator.path(uri)][uri] = object_sha256(entities[uri])
    operations = [
        *reingestion,
        *(identified({"roles": roles}) for _, roles in sorted(roles_by_path.items())),
    ]
    role_types = Counter(
        ROLE_MAP[role] for uri in orphan_uris for role in ids(entities[uri], WITH_ROLE)
    )
    report: dict[str, object] = {
        "version": PLAN_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config_path,
        "config_sha256": sha256(config_path),
        "rdf_dir": config.rdf_dir,
        "api_cache": cache_path,
        "csv_output_dir": csv_output_dir,
        "csv_files": write_provider_csvs(csv_output_dir, rows_by_provider),
        "summary": {
            "existing_roles": len(roles),
            "referenced_roles": len(referenced),
            "orphan_roles": len(orphans),
            "orphan_role_types": dict(sorted(role_types.items())),
            "orphan_roles_without_role_type": sum(
                not ids(entities[uri], WITH_ROLE) for uri in orphan_uris
            ),
            "links_from_orphan_to_referenced_roles": int(
                (orphan_sources & ~orphan_targets).sum()
            ),
            "works_with_lost_roles": len(lost),
            "reingested_works": len(reingestion),
            "blocked_works": len(blocked),
            "blockers": dict(
                sorted(
                    Counter(
                        blocker
                        for entry in blocked
                        for blocker in cast(list[str], entry["blockers"])
                    ).items()
                )
            ),
            "providers": dict(
                sorted(
                    Counter(
                        cast(str, operation["provider"]) for operation in reingestion
                    ).items()
                )
            ),
            "kept_orphan_roles": len(kept),
            "operations": len(operations),
        },
        "operations": operations,
        "operations_sha256": object_sha256(operations),
        "blocked": blocked,
    }
    write_json(report_path, report)
    return report


def _check_preconditions(
    pending: list[dict[str, object]], locator: EntityFileLocator, workers: int
) -> None:
    check_work_preconditions(
        [operation for operation in pending if "br" in operation], locator, workers
    )
    planned = {
        uri: digest
        for operation in pending
        if "roles" in operation
        for uri, digest in cast(dict[str, str], operation["roles"]).items()
    }
    entities = load_available_entities(set(planned), locator, workers)
    for uri, digest in planned.items():
        if uri not in entities:
            raise RuntimeError(f"Stale plan: agent role is missing: {uri}")
        if object_sha256(entities[uri]) != digest:
            raise RuntimeError(f"Stale plan: local RDF state changed for {uri}")


def _delete_orphans(
    editor: MetaEditor, operation: dict[str, object], locator: EntityFileLocator
) -> None:
    roles = cast(dict[str, str], operation["roles"])
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=editor.supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    import_exact_entities(editor, g_set, set(roles), locator)
    for uri in sorted(roles):
        agent_role(g_set, uri).mark_as_to_be_deleted()
    editor.save(g_set, editor.supplier_prefix)


def _apply_operation(
    editor: MetaEditor, operation: dict[str, object], locator: EntityFileLocator
) -> None:
    if "br" in operation:
        remove_work_roles(editor, operation, locator)
    else:
        _delete_orphans(editor, operation, locator)


def execute_plan(
    config_path: str,
    plan_path: str,
    resp_agent: str,
    progress_path: str,
    execution_report_path: str,
    workers: int,
) -> dict[str, object]:
    def check(
        plan: dict[str, object],
        pending: list[dict[str, object]],
        locator: EntityFileLocator,
    ) -> None:
        verify_csv_files(plan)
        _check_preconditions(pending, locator, workers)

    return plan_execution.execute_plan(
        config_path,
        plan_path,
        resp_agent,
        progress_path,
        execution_report_path,
        PLAN_VERSION,
        "Deleting orphan roles",
        " Run Meta for the Crossref and DataCite CSV directories only after "
        "this plan is complete.",
        check,
        _apply_operation,
    )


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Delete agent roles that no bibliographic resource references. "
            "Roles that BR provenance still links to a live BR are planned for "
            "reingestion through Crossref or DataCite CSV input for Meta."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta YAML config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Write a plan only")
    mode.add_argument("--execute", metavar="PLAN", help="Execute a plan")
    parser.add_argument("--report-file", help="Plan path written by --dry-run")
    parser.add_argument(
        "--csv-output-dir", help="Directory for Crossref and DataCite Meta CSVs"
    )
    parser.add_argument("--cache-file", help="SQLite API cache path")
    parser.add_argument("--mailto", help="Contact email sent to metadata APIs")
    parser.add_argument(
        "--refresh-cache", action="store_true", help="Refresh cached API responses"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(os.cpu_count() or 1, 16),
        help="Processes used for local RDF scanning",
    )
    parser.add_argument("-r", "--resp-agent", help="Provenance responsible-agent URI")
    parser.add_argument("--progress-file", help="Execution progress JSON path")
    parser.add_argument("--execution-report", help="Execution result JSON path")
    args = parser.parse_args()

    if args.dry_run:
        if not args.report_file or not args.csv_output_dir or not args.mailto:
            parser.error(
                "--report-file, --csv-output-dir, and --mailto are required with --dry-run"
            )
        report = analyze_orphan_roles(
            args.config,
            args.report_file,
            args.csv_output_dir,
            args.cache_file or f"{args.report_file}.cache.sqlite",
            args.mailto,
            args.workers,
            args.refresh_cache,
        )
        summary = cast(dict[str, object], report["summary"])
        console.print(
            f"Plan written to [cyan]{os.path.abspath(args.report_file)}[/cyan]. "
            f"Orphan roles: [cyan]{summary['orphan_roles']}[/cyan]; reingested "
            f"works: [cyan]{summary['reingested_works']}[/cyan]; blocked works: "
            f"[cyan]{summary['blocked_works']}[/cyan]; operations: "
            f"[cyan]{summary['operations']}[/cyan]."
        )
        return

    if not args.resp_agent:
        parser.error("--resp-agent is required with --execute")
    plan_execution.stop_on_signals()
    plan_path = os.path.abspath(args.execute)
    progress_path = os.path.abspath(args.progress_file or f"{plan_path}.progress.json")
    report_path = os.path.abspath(
        args.execution_report or f"{plan_path}.execution.json"
    )
    result = execute_plan(
        args.config,
        plan_path,
        args.resp_agent,
        progress_path,
        report_path,
        args.workers,
    )
    console.print(
        f"Execution report written to [cyan]{report_path}[/cyan]. "
        f"Completed operations: [cyan]{result['completed_operations']}[/cyan]."
    )
    if result["complete"] is not True:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
