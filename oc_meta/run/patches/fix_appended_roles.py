# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import os
import re
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast

from oc_ocdm.graph import GraphSet
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.agent_metadata import AgentMetadataClient, ApiCache, WorkMetadata
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.rdf_patch import (
    HAS_IDENTIFIER,
    HAS_LITERAL_VALUE,
    HAS_NEXT,
    IS_DOCUMENT_CONTEXT_FOR,
    ROLE_MAP,
    USES_IDENTIFIER_SCHEME,
    WITH_ROLE,
    AuditConfig,
    EntityFileLocator,
    agent_role,
    batches,
    bibliographic_resource,
    data_files,
    ensure_parent,
    first,
    ids,
    import_exact_entities,
    literals,
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
from oc_meta.run.patches.fix_dangling_ars import (
    PROVIDERS,
    WorkProviderClient,
    scan_role_contexts,
    scan_role_links,
    select_provider,
    verify_csv_files,
    work_csv_row,
    write_provider_csvs,
)
from oc_meta.run.patches.plan_execution import identified, object_sha256

PLAN_VERSION = 2
UPDATE_QUERY = "https://w3id.org/oc/ontology/hasUpdateQuery"
GENERATED_AT = "http://www.w3.org/ns/prov#generatedAtTime"
DOI_SCHEME = "http://purl.org/spar/datacite/doi"
UPDATE_PART = re.compile(r"(?:^|\} \} ?; )(?=(?:DELETE|INSERT) DATA \{ GRAPH <)")
CONTEXT_TRIPLE = re.compile(
    r"<[^>]+> <" + re.escape(IS_DOCUMENT_CONTEXT_FOR) + r"> <([^>]+)>"
)
IDENTIFIER_TRIPLE = re.compile(
    r"<[^>]+> <" + re.escape(HAS_IDENTIFIER) + r"> <([^>]+)>"
)


@dataclass(frozen=True, slots=True)
class RoleSummary:
    role_types: tuple[str, ...]
    next_roles: tuple[str, ...]


def _literal(entity: dict[str, object], predicate: str) -> str:
    return cast(list[dict[str, str]], entity[predicate])[0]["@value"]


def triple_changes(query: str, triple: re.Pattern[str]) -> tuple[set[str], set[str]]:
    inserted: set[str] = set()
    deleted: set[str] = set()
    for part in UPDATE_PART.split(query):
        target = inserted if part.startswith("INSERT") else deleted
        target.update(match.group(1) for match in triple.finditer(part))
    return inserted, deleted


def updates_newest_first(
    snapshots: list[tuple[int, dict[str, object]]],
) -> list[tuple[int, dict[str, object], str]]:
    return [
        (number, snapshot, _literal(snapshot, UPDATE_QUERY))
        for number, snapshot in sorted(
            snapshots, key=lambda item: item[0], reverse=True
        )
        if number >= 2 and UPDATE_QUERY in snapshot
    ]


def _append_events(
    roles: set[str], snapshots: list[tuple[int, dict[str, object]]]
) -> list[dict[str, object]]:
    events = []
    role_state = set(roles)
    for number, snapshot, query in updates_newest_first(snapshots):
        inserted, deleted = triple_changes(query, CONTEXT_TRIPLE)
        before = (role_state - inserted) | deleted
        added = inserted & roles
        prior = before & roles
        if added and prior:
            events.append(
                {
                    "snapshot": number,
                    "generated_at": _literal(snapshot, GENERATED_AT),
                    "added": sorted(added),
                    "prior": sorted(prior),
                }
            )
        role_state = before
    return events


def original_identifiers(
    identifiers: set[str], snapshots: list[tuple[int, dict[str, object]]]
) -> set[str]:
    state = set(identifiers)
    for _, _, query in updates_newest_first(snapshots):
        inserted, deleted = triple_changes(query, IDENTIFIER_TRIPLE)
        state = (state - inserted) | deleted
    return state & identifiers


def _scan_work_file(path: str, zip_output: bool) -> list[dict[str, object]]:
    works = {
        uri: entity
        for uri, entity in load_entities(path).items()
        if IS_DOCUMENT_CONTEXT_FOR in entity
    }
    prov_path = provenance_path(path, zip_output)
    if not works or not os.path.exists(prov_path):
        return []
    snapshots: dict[str, list[tuple[int, dict[str, object]]]] = defaultdict(list)
    for uri, entity in load_entities(prov_path).items():
        work = uri.split("/prov/se/")[0]
        if work in works:
            snapshots[work].append((snapshot_number(uri), entity))
    records = []
    for work, entity in works.items():
        roles = set(ids(entity, IS_DOCUMENT_CONTEXT_FOR))
        identifiers = set(ids(entity, HAS_IDENTIFIER))
        events = _append_events(roles, snapshots[work])
        if events:
            records.append(
                {
                    "br": work,
                    "roles": sorted(roles),
                    "identifiers": sorted(identifiers),
                    "original_identifiers": sorted(
                        original_identifiers(identifiers, snapshots[work])
                    ),
                    "events": events,
                }
            )
    return records


def _scan_work_batch(task: tuple[list[str], bool]) -> list[dict[str, object]]:
    paths, zip_output = task
    return [record for path in paths for record in _scan_work_file(path, zip_output)]


def _new_pool(workers: int) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(max_workers=workers, mp_context=mp_method)


def find_append_candidates(
    config: AuditConfig, workers: int
) -> list[dict[str, object]]:
    files = data_files(os.path.join(config.rdf_dir, "br"), config.zip_output)
    tasks = [(batch, config.zip_output) for batch in batches(files, 8)]
    candidates = []
    with create_progress() as progress:
        task = progress.add_task("Reading BR provenance", total=len(files))
        for (paths, _), records in zip(
            tasks,
            recycled_map(_scan_work_batch, tasks, workers, lambda: _new_pool(workers)),
        ):
            candidates.extend(records)
            progress.advance(task, len(paths))
    return candidates


def _summarize_role_batch(
    tasks: list[tuple[str, frozenset[str]]],
) -> dict[str, RoleSummary]:
    summaries = {}
    for path, targets in tasks:
        if not os.path.exists(path):
            continue
        for uri, entity in load_entities(path).items():
            if uri in targets:
                summaries[uri] = RoleSummary(
                    tuple(ids(entity, WITH_ROLE)), tuple(ids(entity, HAS_NEXT))
                )
    return summaries


def load_role_summaries(
    uris: set[str], locator: EntityFileLocator, workers: int
) -> dict[str, RoleSummary]:
    targets_by_path: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        targets_by_path[locator.path(uri)].add(uri)
    tasks = [(path, frozenset(targets)) for path, targets in targets_by_path.items()]
    summaries = {}
    with create_progress() as progress:
        task = progress.add_task("Reading agent roles", total=len(tasks))
        task_batches = batches(tasks, 24)
        for batch, partial in zip(
            task_batches,
            recycled_map(
                _summarize_role_batch, task_batches, workers, lambda: _new_pool(workers)
            ),
        ):
            summaries.update(partial)
            progress.advance(task, len(batch))
    return summaries


def doubled_roles(
    candidate: dict[str, object], summaries: dict[str, RoleSummary]
) -> tuple[dict[str, list[str]], list[str]]:
    roles = set(cast(list[str], candidate["roles"]))
    if any(role not in summaries for role in roles):
        return {}, ["missing_role"]
    if any(
        len(summaries[role].role_types) != 1
        or summaries[role].role_types[0] not in ROLE_MAP
        for role in roles
    ):
        return {}, ["malformed_role"]
    by_type: dict[str, set[str]] = defaultdict(set)
    for role in roles:
        by_type[ROLE_MAP[summaries[role].role_types[0]]].add(role)
    doubled = {}
    for role_type, typed in sorted(by_type.items()):
        if any(
            typed.intersection(cast(list[str], event["added"]))
            and typed.intersection(cast(list[str], event["prior"]))
            for event in cast(list[dict[str, object]], candidate["events"])
        ):
            doubled[role_type] = sorted(typed)
    return doubled, []


def doi_order(
    candidate: dict[str, object], identifiers: dict[str, dict[str, object]]
) -> list[str]:
    original = set(cast(list[str], candidate["original_identifiers"]))
    ranked: dict[str, int] = {}
    for uri in cast(list[str], candidate["identifiers"]):
        if uri not in identifiers:
            continue
        entity = identifiers[uri]
        if first(ids(entity, USES_IDENTIFIER_SCHEME)) != DOI_SCHEME:
            continue
        doi = first(literals(entity, HAS_LITERAL_VALUE))
        rank = 0 if uri in original else 1
        ranked[doi] = min(rank, ranked[doi]) if doi in ranked else rank
    return sorted(ranked, key=lambda doi: (ranked[doi], doi))


def choose_record(
    provider: WorkProviderClient, dois: list[str]
) -> tuple[str, str | None, WorkMetadata | None, list[dict[str, object]]]:
    attempts: list[dict[str, object]] = []
    for doi in dois:
        selected, metadata, attempted, errors = select_provider(provider, doi)
        attempts.append({"doi": doi, "attempted": attempted, "errors": errors})
        if metadata is not None:
            return doi, selected, metadata, attempts
    return "", None, None, attempts


def missing_role_types(metadata: WorkMetadata, role_types: list[str]) -> list[str]:
    present = {
        "author": bool(metadata["author"]),
        "editor": bool(metadata["editor"]),
        "publisher": bool(metadata["publisher"]),
    }
    return [role_type for role_type in role_types if not present[role_type]]


def work_roles(work: dict[str, object]) -> set[str]:
    return {
        role
        for roles in cast(dict[str, list[str]], work["role_types"]).values()
        for role in roles
    }


def entity_hashes(
    work: dict[str, object], entities: dict[str, dict[str, object]]
) -> dict[str, str]:
    uris = {cast(str, work["br"]), *work_roles(work)}
    return {uri: object_sha256(entities[uri]) for uri in sorted(uris)}


def _local_blockers(
    works: list[dict[str, object]], config: AuditConfig, workers: int
) -> dict[str, list[str]]:
    target_roles = {role for work in works for role in work_roles(work)}
    contexts = scan_role_contexts(config, target_roles, workers)
    links = scan_role_links(config, target_roles, workers)
    owners = {
        role: cast(str, work["br"]) for work in works for role in work_roles(work)
    }
    blockers: dict[str, list[str]] = defaultdict(list)
    for role, owner in owners.items():
        if contexts[role] != (owner,) and "shared_role" not in blockers[owner]:
            blockers[owner].append("shared_role")
    for source, target in links:
        source_owner = owners[source] if source in owners else None
        target_owner = owners[target] if target in owners else None
        if source_owner == target_owner:
            continue
        for owner in (source_owner, target_owner):
            if owner is not None and "external_has_next" not in blockers[owner]:
                blockers[owner].append("external_has_next")
    return blockers


def analyze_appended_roles(
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
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    candidates = find_append_candidates(config, workers)
    summaries = load_role_summaries(
        {
            role
            for candidate in candidates
            for role in cast(list[str], candidate["roles"])
        },
        locator,
        workers,
    )

    blocked: list[dict[str, object]] = []
    works = []
    for candidate in candidates:
        doubled, blockers = doubled_roles(candidate, summaries)
        if blockers:
            blocked.append({"br": candidate["br"], "blockers": blockers})
        elif doubled:
            works.append({"br": candidate["br"], "role_types": doubled, **candidate})

    local_blockers = _local_blockers(works, config, workers)
    works = [work for work in works if not local_blockers[cast(str, work["br"])]]
    blocked.extend(
        {"br": work_uri, "blockers": blockers}
        for work_uri, blockers in local_blockers.items()
        if blockers
    )
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
                        }
                    )
                    continue
                operation: dict[str, object] = {
                    "br": work_uri,
                    "doi": doi,
                    "provider": selected,
                    "role_types": role_types,
                    "snapshots": sorted(
                        cast(int, event["snapshot"])
                        for event in cast(list[dict[str, object]], work["events"])
                    ),
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
    deleted_by_type: Counter[str] = Counter()
    for operation in operations:
        for role_type, roles in cast(
            dict[str, list[str]], operation["role_types"]
        ).items():
            deleted_by_type[role_type] += len(roles)
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
            "append_candidates": len(candidates),
            "operations": len(operations),
            "blocked_brs": len(blocked),
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
                        cast(str, operation["provider"]) for operation in operations
                    ).items()
                )
            ),
            "roles_to_delete": dict(sorted(deleted_by_type.items())),
        },
        "operations": operations,
        "operations_sha256": object_sha256(operations),
        "blocked": blocked,
    }
    write_json(report_path, report)
    return report


def check_work_preconditions(
    operations: list[dict[str, object]], locator: EntityFileLocator, workers: int
) -> None:
    entities = load_available_entities(
        {
            uri
            for operation in operations
            for uri in cast(dict[str, str], operation["entities_sha256"])
        },
        locator,
        workers,
    )
    for operation in operations:
        planned = cast(dict[str, str], operation["entities_sha256"])
        if any(uri not in entities for uri in planned):
            raise RuntimeError(
                f"Stale plan: an entity is missing for {operation['br']}"
            )
        if entity_hashes(operation, entities) != planned:
            raise RuntimeError(
                f"Stale plan: local RDF state changed for {operation['br']}"
            )


def remove_work_roles(
    editor: MetaEditor, operation: dict[str, object], locator: EntityFileLocator
) -> None:
    deleted = sorted(work_roles(operation))
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=editor.supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    work_uri = cast(str, operation["br"])
    import_exact_entities(editor, g_set, {work_uri, *deleted}, locator)
    work = bibliographic_resource(g_set, work_uri)
    for role_uri in deleted:
        role = agent_role(g_set, role_uri)
        work.remove_contributor(role)
        role.mark_as_to_be_deleted()
    editor.save(g_set, editor.supplier_prefix)


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
        check_work_preconditions(pending, locator, workers)

    return plan_execution.execute_plan(
        config_path,
        plan_path,
        resp_agent,
        progress_path,
        execution_report_path,
        PLAN_VERSION,
        "Removing doubled roles",
        " Run Meta for the Crossref and DataCite CSV directories only after "
        "this plan is complete.",
        check,
        remove_work_roles,
    )


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Find contributor roles that earlier merges appended to an existing "
            "role chain of the same type, remove every role of the doubled types, "
            "and prepare Crossref or DataCite CSV input for Meta."
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
        report = analyze_appended_roles(
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
            f"Operations: [cyan]{summary['operations']}[/cyan]; blocked BRs: "
            f"[cyan]{summary['blocked_brs']}[/cyan]."
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
