# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import csv
import hashlib
import multiprocessing
import os
import shutil
import signal
import tempfile
from collections import Counter, defaultdict
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, cast

import orjson
import requests
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic.bibliographic_resource import (
    BibliographicResource,
)
from oc_ocdm.graph.graph_entity import GraphEntity
from rich_argparse import RichHelpFormatter
from triplelite import RDFTerm, TripleLite, from_rdflib

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.agent_metadata import (
    AgentIdentifier,
    AgentMetadata,
    AgentMetadataClient,
    ApiCache,
    WorkMetadata,
)
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.rdf_patch import (
    HAS_IDENTIFIER,
    HAS_LITERAL_VALUE,
    HAS_NEXT,
    IS_DOCUMENT_CONTEXT_FOR,
    PROV_SPECIALIZATION_OF,
    USES_IDENTIFIER_SCHEME,
    AuditConfig,
    EntityFileLocator,
    batches as _batches,
    data_files as _data_files,
    ensure_parent as _ensure_parent,
    first as _first,
    ids as _ids,
    literals as _literals,
    load_audit_config,
    load_available_entities,
    load_entities as _load_entities,
    provenance_path as _provenance_path,
    read_json_object as _read_json_object,
    sha256 as _sha256,
    snapshot_number as _snapshot_number,
    write_json as _write_json,
)
from oc_meta.run.merge.entities import REINDEX_SENTINEL_FILENAME
from oc_meta.run.meta.generate_csv import FIELDNAMES, URI_TYPE_DICT

PLAN_VERSION = 2
PROVIDERS = ("crossref", "datacite")
PROV_INVALIDATED_AT_TIME = "http://www.w3.org/ns/prov#invalidatedAtTime"
DCTERMS_TITLE = "http://purl.org/dc/terms/title"

csv.field_size_limit(2**31 - 1)

_stop_requested = False
_existing_roles: frozenset[str] = frozenset()
_target_roles: frozenset[str] = frozenset()
_fork_context = multiprocessing.get_context("fork")
_context = multiprocessing.get_context("forkserver")


@dataclass(frozen=True, slots=True)
class WorkRecord:
    uri: str
    role_uris: tuple[str, ...]
    identifier_uris: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IdentifierRecord:
    uri: str
    scheme: str
    value: str


class WorkProviderClient(Protocol):
    def crossref(self, doi: str) -> WorkMetadata | None: ...

    def datacite(self, doi: str) -> WorkMetadata | None: ...


def _handle_signal(signum: int, frame: object) -> None:
    del signum, frame
    global _stop_requested
    _stop_requested = True


def _object_sha256(value: object) -> str:
    return hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()


def _process_pool(workers: int) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(max_workers=workers, mp_context=_fork_context)


def _scan_entity_uri_batch(paths: list[str]) -> set[str]:
    return {uri for path in paths for uri in _load_entities(path)}


def scan_entity_uris(files: list[str], workers: int) -> set[str]:
    result = set()
    batches = _batches(files, 24)
    with _process_pool(workers) as executor:
        partial_results = executor.map(_scan_entity_uri_batch, batches)
        with create_progress() as progress:
            task = progress.add_task("Indexing agent roles", total=len(files))
            for batch, partial in zip(batches, partial_results):
                result.update(partial)
                progress.advance(task, len(batch))
    return result


def _init_dangling_scan(existing_roles: frozenset[str]) -> None:
    global _existing_roles
    _existing_roles = existing_roles


def _scan_dangling_work_batch(
    paths: list[str],
) -> tuple[dict[str, WorkRecord], dict[str, tuple[str, ...]]]:
    works = {}
    missing_by_work = {}
    for path in paths:
        for uri, entity in _load_entities(path).items():
            role_uris = tuple(_ids(entity, IS_DOCUMENT_CONTEXT_FOR))
            missing = tuple(sorted(set(role_uris) - _existing_roles))
            if not missing:
                continue
            works[uri] = WorkRecord(uri, role_uris, tuple(_ids(entity, HAS_IDENTIFIER)))
            missing_by_work[uri] = missing
    return works, missing_by_work


def _init_role_scan(target_roles: frozenset[str]) -> None:
    global _target_roles
    _target_roles = target_roles


def _scan_role_context_batch(paths: list[str]) -> dict[str, list[str]]:
    contexts: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        for uri, entity in _load_entities(path).items():
            for role_uri in _ids(entity, IS_DOCUMENT_CONTEXT_FOR):
                if role_uri in _target_roles:
                    contexts[role_uri].append(uri)
    return contexts


def _scan_role_link_batch(paths: list[str]) -> set[tuple[str, str]]:
    links = set()
    for path in paths:
        for uri, entity in _load_entities(path).items():
            for next_uri in _ids(entity, HAS_NEXT):
                if uri in _target_roles or next_uri in _target_roles:
                    links.add((uri, next_uri))
    return links


def _scan_contexts(
    br_files: list[str], target_roles: frozenset[str], workers: int
) -> dict[str, tuple[str, ...]]:
    contexts: dict[str, list[str]] = defaultdict(list)
    batches = _batches(br_files, 24)
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=_fork_context,
        initializer=_init_role_scan,
        initargs=(target_roles,),
    ) as executor:
        partial_results = executor.map(_scan_role_context_batch, batches)
        with create_progress() as progress:
            task = progress.add_task("Checking role contexts", total=len(br_files))
            for batch, partial in zip(batches, partial_results):
                for role_uri, work_uris in partial.items():
                    contexts[role_uri].extend(work_uris)
                progress.advance(task, len(batch))
    return {role_uri: tuple(sorted(contexts[role_uri])) for role_uri in target_roles}


def find_dangling_works(
    config: AuditConfig, workers: int
) -> tuple[
    dict[str, WorkRecord],
    dict[str, dict[str, object]],
    dict[str, tuple[str, ...]],
    dict[str, tuple[str, ...]],
]:
    role_files = _data_files(os.path.join(config.rdf_dir, "ar"), config.zip_output)
    existing_roles = frozenset(scan_entity_uris(role_files, workers))
    br_files = _data_files(os.path.join(config.rdf_dir, "br"), config.zip_output)
    works = {}
    missing_by_work = {}
    batches = _batches(br_files, 24)
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=_fork_context,
        initializer=_init_dangling_scan,
        initargs=(existing_roles,),
    ) as executor:
        partial_results = executor.map(_scan_dangling_work_batch, batches)
        with create_progress() as progress:
            task = progress.add_task(
                "Finding dangling role references", total=len(br_files)
            )
            for batch, (partial_works, partial_missing) in zip(
                batches, partial_results
            ):
                works.update(partial_works)
                missing_by_work.update(partial_missing)
                progress.advance(task, len(batch))
    target_roles = frozenset(
        role_uri for work in works.values() for role_uri in work.role_uris
    )
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    role_entities = load_available_entities(
        set(target_roles.intersection(existing_roles)), locator, workers
    )
    return (
        works,
        role_entities,
        missing_by_work,
        _scan_contexts(br_files, target_roles, workers),
    )


def scan_role_links(
    config: AuditConfig, role_uris: set[str], workers: int
) -> set[tuple[str, str]]:
    if not role_uris:
        return set()
    role_files = _data_files(os.path.join(config.rdf_dir, "ar"), config.zip_output)
    batches = _batches(role_files, 24)
    links = set()
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=_fork_context,
        initializer=_init_role_scan,
        initargs=(frozenset(role_uris),),
    ) as executor:
        partial_results = executor.map(_scan_role_link_batch, batches)
        with create_progress() as progress:
            task = progress.add_task("Checking role links", total=len(role_files))
            for batch, partial in zip(batches, partial_results):
                links.update(partial)
                progress.advance(task, len(batch))
    return links


def _identifier_record(uri: str, entity: dict[str, object]) -> IdentifierRecord | None:
    scheme_uri = _first(_ids(entity, USES_IDENTIFIER_SCHEME))
    value = _first(_literals(entity, HAS_LITERAL_VALUE))
    if not scheme_uri or not value:
        return None
    return IdentifierRecord(uri, scheme_uri.rsplit("/", 1)[-1], value)


def _provenance_status_batch(
    paths: list[tuple[str, frozenset[str]]],
) -> dict[str, str]:
    snapshots_by_entity: dict[str, list[dict[str, object]]] = defaultdict(list)
    for path, targets in paths:
        if not os.path.exists(path):
            continue
        for snapshot in _load_entities(path).values():
            specialization = _first(_ids(snapshot, PROV_SPECIALIZATION_OF))
            if specialization in targets:
                snapshots_by_entity[specialization].append(snapshot)
    statuses = {}
    for uri, snapshots in snapshots_by_entity.items():
        latest = max(
            snapshots,
            key=lambda snapshot: _snapshot_number(cast(str, snapshot["@id"])),
        )
        statuses[uri] = (
            "latest_snapshot_invalidated"
            if _literals(latest, PROV_INVALIDATED_AT_TIME)
            else "latest_snapshot_active"
        )
    return statuses


def load_provenance_statuses(
    uris: set[str], locator: EntityFileLocator, workers: int
) -> dict[str, str]:
    targets_by_path: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        path = _provenance_path(locator.path(uri), locator.zip_output)
        targets_by_path[path].add(uri)
    tasks = [(path, frozenset(targets)) for path, targets in targets_by_path.items()]
    statuses = {}
    with ProcessPoolExecutor(
        max_workers=workers, mp_context=_context
    ) as executor:
        for partial in executor.map(_provenance_status_batch, _batches(tasks, 24)):
            statuses.update(partial)
    return {uri: statuses[uri] if uri in statuses else "no_snapshot" for uri in uris}


def _load_local_context(
    works: dict[str, WorkRecord],
    role_entities: dict[str, dict[str, object]],
    missing_by_work: dict[str, tuple[str, ...]],
    locator: EntityFileLocator,
    workers: int,
) -> tuple[dict[str, IdentifierRecord], dict[str, dict[str, object]], dict[str, str]]:
    work_entities = load_available_entities(set(works), locator, workers)
    identifier_uris = {
        identifier_uri
        for work in works.values()
        for identifier_uri in work.identifier_uris
    }
    identifier_entities = load_available_entities(identifier_uris, locator, workers)
    identifiers = {
        uri: record
        for uri, entity in identifier_entities.items()
        if (record := _identifier_record(uri, entity)) is not None
    }
    raw_entities = dict(work_entities)
    raw_entities.update(role_entities)
    raw_entities.update(identifier_entities)
    missing_uris = {uri for uris in missing_by_work.values() for uri in uris}
    provenance = load_provenance_statuses(missing_uris, locator, workers)
    return identifiers, raw_entities, provenance


def _work_identifiers(
    work: WorkRecord, identifiers: Mapping[str, IdentifierRecord]
) -> dict[str, str]:
    result = {}
    for uri in work.identifier_uris:
        if uri in identifiers and identifiers[uri].scheme not in result:
            record = identifiers[uri]
            result[record.scheme] = record.value
    return result


def _types(entity: dict[str, object]) -> list[str]:
    value = entity["@type"] if "@type" in entity else []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _local_type(entity: dict[str, object]) -> str:
    for entity_type in _types(entity):
        mapped = URI_TYPE_DICT[entity_type] if entity_type in URI_TYPE_DICT else ""
        if mapped:
            return mapped
    return ""


def _omid(br_uri: str) -> str:
    if "/br/" not in br_uri:
        raise ValueError(f"Cannot derive a BR OMID from URI: {br_uri}")
    return f"omid:br/{br_uri.rsplit('/br/', 1)[1]}"


def _serialize_name(
    family: str,
    given: str,
    name: str,
    identifiers: tuple[AgentIdentifier, ...],
) -> str:
    if family or given:
        rendered_name = f"{family}, {given}" if given else f"{family},"
    else:
        rendered_name = name
    rendered_identifiers = " ".join(
        f"{identifier['scheme']}:{identifier['value']}" for identifier in identifiers
    )
    if rendered_identifiers:
        return (
            f"{rendered_name} [{rendered_identifiers}]"
            if rendered_name
            else f"[{rendered_identifiers}]"
        )
    return rendered_name


def serialize_agents(agents: list[AgentMetadata]) -> str:
    return "; ".join(
        _serialize_name(
            agent["family"],
            agent["given"],
            agent["name"],
            agent["identifiers"],
        )
        for agent in agents
    )


def work_csv_row(
    work: WorkRecord,
    work_entity: dict[str, object],
    identifiers: Mapping[str, str],
    metadata: WorkMetadata,
) -> dict[str, str]:
    doi = identifiers["doi"]
    publisher = _serialize_name(
        "",
        "",
        metadata["publisher"],
        metadata["publisher_identifiers"],
    )
    return {
        "id": f"{_omid(work.uri)} doi:{doi}",
        "title": _first(_literals(work_entity, DCTERMS_TITLE)),
        "author": serialize_agents(metadata["author"]),
        "issue": "",
        "volume": "",
        "venue": "",
        "page": "",
        "pub_date": "",
        "type": _local_type(work_entity),
        "publisher": publisher,
        "editor": serialize_agents(metadata["editor"]),
    }


def _select_provider(
    provider: WorkProviderClient, doi: str
) -> tuple[
    str | None,
    WorkMetadata | None,
    list[str],
    list[dict[str, str]],
]:
    attempted = []
    errors = []
    if not doi:
        return None, None, attempted, errors
    attempted.append("crossref")
    try:
        metadata = provider.crossref(doi)
    except requests.RequestException as error:
        errors.append(
            {
                "provider": "crossref",
                "error": type(error).__name__,
                "message": str(error),
            }
        )
        metadata = None
    if metadata is not None:
        return "crossref", metadata, attempted, errors
    attempted.append("datacite")
    try:
        metadata = provider.datacite(doi)
    except requests.RequestException as error:
        errors.append(
            {
                "provider": "datacite",
                "error": type(error).__name__,
                "message": str(error),
            }
        )
        metadata = None
    if metadata is not None:
        return "datacite", metadata, attempted, errors
    return None, None, attempted, errors


def _operation_payload(operation: dict[str, object]) -> dict[str, object]:
    payload = dict(operation)
    payload.pop("operation_id")
    return payload


def _finalize_operation(operation: dict[str, object]) -> dict[str, object]:
    operation["operation_id"] = _object_sha256(operation)[:20]
    return operation


def _verify_operation_id(operation: dict[str, object]) -> None:
    operation_id = operation["operation_id"]
    if not isinstance(operation_id, str):
        raise ValueError("Operation ID must be a string")
    if _object_sha256(_operation_payload(operation))[:20] != operation_id:
        raise ValueError(f"Correction plan has modified operation {operation_id}")


def _operation_preconditions(
    work: WorkRecord,
    role_entities: Mapping[str, dict[str, object]],
    raw_entities: Mapping[str, dict[str, object]],
    missing: tuple[str, ...],
    contexts: Mapping[str, tuple[str, ...]],
    links: set[tuple[str, str]],
    owned_missing: set[str],
    provenance: Mapping[str, str],
) -> dict[str, object]:
    return {
        "br_entity": raw_entities[work.uri],
        "identifier_entities": {
            uri: raw_entities[uri]
            for uri in sorted(work.identifier_uris)
            if uri in raw_entities
        },
        "role_entities": {
            uri: role_entities[uri]
            for uri in sorted(work.role_uris)
            if uri in role_entities
        },
        "role_references": list(work.role_uris),
        "dangling_ar_references": list(missing),
        "owned_missing_provenance": {
            uri: provenance[uri] for uri in sorted(owned_missing)
        },
        "contexts": {uri: list(contexts[uri]) for uri in sorted(set(work.role_uris))},
        "has_next_edges": [
            [source, target]
            for source, target in sorted(links)
            if source in work.role_uris or target in work.role_uris
        ],
    }


def build_repair_plan(
    works: dict[str, WorkRecord],
    role_entities: dict[str, dict[str, object]],
    identifiers: dict[str, IdentifierRecord],
    raw_entities: dict[str, dict[str, object]],
    missing_by_work: dict[str, tuple[str, ...]],
    contexts: dict[str, tuple[str, ...]],
    provenance: dict[str, str],
    links: set[tuple[str, str]],
    provider: WorkProviderClient,
) -> tuple[list[dict[str, object]], dict[str, list[dict[str, str]]]]:
    affected_brs = set(works)
    target_roles = {role_uri for work in works.values() for role_uri in work.role_uris}
    owners = {
        role_uri: min(
            br_uri for br_uri, work in works.items() if role_uri in work.role_uris
        )
        for role_uri in target_roles
    }
    boundary_links = {
        edge for edge in links if (edge[0] in target_roles) != (edge[1] in target_roles)
    }
    operations = []
    rows_by_provider: dict[str, list[dict[str, str]]] = {
        provider_name: [] for provider_name in PROVIDERS
    }
    with create_progress() as progress:
        task = progress.add_task("Querying work metadata", total=len(works))
        for br_uri in sorted(works):
            if _stop_requested:
                break
            work = works[br_uri]
            work_identifiers = _work_identifiers(work, identifiers)
            blockers = []
            for role_uri in sorted(set(work.role_uris)):
                external_contexts = sorted(set(contexts[role_uri]) - affected_brs)
                if external_contexts:
                    blockers.append(
                        {
                            "type": "shared_agent_role",
                            "ar": role_uri,
                            "external_contexts": external_contexts,
                        }
                    )
            for source, target in sorted(boundary_links):
                if source in work.role_uris or target in work.role_uris:
                    blockers.append(
                        {
                            "type": "external_has_next",
                            "source": source,
                            "target": target,
                        }
                    )
            selected_provider = None
            metadata = None
            attempted_providers: list[str] = []
            provider_errors: list[dict[str, str]] = []
            if not blockers:
                (
                    selected_provider,
                    metadata,
                    attempted_providers,
                    provider_errors,
                ) = _select_provider(
                    provider,
                    work_identifiers["doi"] if "doi" in work_identifiers else "",
                )
                if metadata is None:
                    blockers.append(
                        {
                            "type": "provider_unavailable",
                            "doi": work_identifiers["doi"]
                            if "doi" in work_identifiers
                            else "",
                            "attempted_providers": attempted_providers,
                            "errors": provider_errors,
                        }
                    )
            existing_roles = sorted(
                role_uri
                for role_uri in set(work.role_uris)
                if role_uri in role_entities and owners[role_uri] == br_uri
            )
            owned_missing = {
                role_uri
                for role_uri in missing_by_work[br_uri]
                if owners[role_uri] == br_uri
            }
            invalidate_missing = sorted(
                role_uri
                for role_uri in owned_missing
                if provenance[role_uri] == "latest_snapshot_active"
            )
            work_entity = raw_entities[br_uri]
            operation = _finalize_operation(
                {
                    "work": {
                        "br": br_uri,
                        "title": _first(_literals(work_entity, DCTERMS_TITLE)),
                        "type": _local_type(work_entity),
                        "identifiers": work_identifiers,
                    },
                    "provider": {
                        "selected": selected_provider,
                        "attempted": attempted_providers,
                        "errors": provider_errors,
                        "author_count": len(metadata["author"])
                        if metadata is not None
                        else 0,
                        "editor_count": len(metadata["editor"])
                        if metadata is not None
                        else 0,
                        "publisher_present": bool(metadata["publisher"])
                        if metadata is not None
                        else False,
                    },
                    "blockers": blockers,
                    "actions": {
                        "remove_role_references": list(work.role_uris),
                        "delete_existing_ars": existing_roles,
                        "invalidate_missing_ars": invalidate_missing,
                    },
                    "preconditions": _operation_preconditions(
                        work,
                        role_entities,
                        raw_entities,
                        missing_by_work[br_uri],
                        contexts,
                        links,
                        owned_missing,
                        provenance,
                    ),
                }
            )
            operations.append(operation)
            if metadata is not None and selected_provider is not None and not blockers:
                rows_by_provider[selected_provider].append(
                    work_csv_row(work, work_entity, work_identifiers, metadata)
                )
            progress.advance(task)
    return operations, rows_by_provider


def _write_csv(path: str, rows: list[dict[str, str]]) -> None:
    _ensure_parent(path)
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_provider_csvs(
    output_dir: str, rows_by_provider: Mapping[str, list[dict[str, str]]]
) -> dict[str, dict[str, object]]:
    output_dir = os.path.abspath(output_dir)
    parent = os.path.dirname(output_dir)
    os.makedirs(parent, exist_ok=True)
    staging_dir = tempfile.mkdtemp(
        prefix=f".{os.path.basename(output_dir)}.", dir=parent
    )
    backup_dir = f"{staging_dir}.previous"
    try:
        for provider_name in PROVIDERS:
            _write_csv(
                os.path.join(staging_dir, provider_name, "input.csv"),
                rows_by_provider[provider_name],
            )
        if os.path.exists(output_dir):
            os.replace(output_dir, backup_dir)
        try:
            os.replace(staging_dir, output_dir)
        except OSError:
            if os.path.exists(backup_dir):
                os.replace(backup_dir, output_dir)
            raise
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)
    finally:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
    return {
        provider_name: {
            "path": os.path.join(output_dir, provider_name, "input.csv"),
            "sha256": _sha256(os.path.join(output_dir, provider_name, "input.csv")),
            "rows": len(rows_by_provider[provider_name]),
        }
        for provider_name in PROVIDERS
    }


def analyze_dangling_ars(
    config_path: str,
    report_path: str,
    csv_output_dir: str,
    cache_path: str,
    mailto: str,
    workers: int,
    refresh_cache: bool,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    config_path = os.path.abspath(config_path)
    report_path = os.path.abspath(report_path)
    csv_output_dir = os.path.abspath(csv_output_dir)
    cache_path = os.path.abspath(cache_path)
    config = load_audit_config(config_path)
    works, role_entities, missing_by_work, contexts = find_dangling_works(
        config, workers
    )
    target_roles = {role_uri for work in works.values() for role_uri in work.role_uris}
    links = scan_role_links(config, target_roles, workers)
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    identifiers, raw_entities, provenance = _load_local_context(
        works, role_entities, missing_by_work, locator, workers
    )
    _ensure_parent(cache_path)
    api_cache = ApiCache(cache_path)
    provider = AgentMetadataClient(
        mailto=mailto, cache=api_cache, refresh_cache=refresh_cache
    )
    try:
        operations, rows_by_provider = build_repair_plan(
            works,
            role_entities,
            identifiers,
            raw_entities,
            missing_by_work,
            contexts,
            provenance,
            links,
            provider,
        )
    finally:
        provider.close()
        api_cache.close()
    blockers = [
        blocker
        for operation in operations
        for blocker in cast(list[dict[str, object]], operation["blockers"])
    ]
    complete = not _stop_requested and len(operations) == len(works)
    executable = complete and not blockers
    csv_files = (
        write_provider_csvs(csv_output_dir, rows_by_provider) if executable else {}
    )
    report: dict[str, object] = {
        "version": PLAN_VERSION,
        "complete": complete,
        "executable": executable,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config_path,
        "config_sha256": _sha256(config_path),
        "rdf_dir": config.rdf_dir,
        "api_cache": cache_path,
        "csv_output_dir": csv_output_dir,
        "csv_files": csv_files,
        "operations_sha256": _object_sha256(operations),
        "summary": {
            "affected_brs": len(works),
            "planned_operations": len(operations),
            "dangling_ar_references": len(
                {uri for missing in missing_by_work.values() for uri in missing}
            ),
            "existing_ars_to_delete": len(role_entities),
            "blocker_counts": dict(
                sorted(
                    Counter(cast(str, blocker["type"]) for blocker in blockers).items()
                )
            ),
            "provider_counts": dict(
                sorted(
                    Counter(
                        cast(
                            str,
                            cast(dict[str, object], operation["provider"])["selected"],
                        )
                        for operation in operations
                        if cast(dict[str, object], operation["provider"])["selected"]
                        is not None
                    ).items()
                )
            ),
        },
        "operations": operations,
    }
    _write_json(report_path, report)
    return report


def _plan_operations(plan: dict[str, object]) -> list[dict[str, object]]:
    if "version" not in plan or plan["version"] != PLAN_VERSION:
        version = plan["version"] if "version" in plan else "missing"
        raise ValueError(
            f"Unsupported correction plan version: {version}; expected {PLAN_VERSION}"
        )
    raw_operations = plan["operations"]
    if not isinstance(raw_operations, list) or not all(
        isinstance(operation, dict) for operation in raw_operations
    ):
        raise ValueError("Correction plan operations must be a list of objects")
    operations = cast(list[dict[str, object]], raw_operations)
    for operation in operations:
        _verify_operation_id(operation)
    if plan["operations_sha256"] != _object_sha256(operations):
        raise ValueError("Correction plan operations were modified")
    return operations


def _verify_csv_files(plan: dict[str, object]) -> None:
    csv_output_dir = plan["csv_output_dir"]
    csv_files = plan["csv_files"]
    if not isinstance(csv_output_dir, str) or not isinstance(csv_files, dict):
        raise ValueError("Correction plan CSV metadata is invalid")
    if set(csv_files) != set(PROVIDERS):
        raise ValueError("Correction plan must contain Crossref and DataCite CSVs")
    for provider_name in PROVIDERS:
        entry = csv_files[provider_name]
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid CSV metadata for {provider_name}")
        expected_path = os.path.join(csv_output_dir, provider_name, "input.csv")
        if entry["path"] != expected_path:
            raise ValueError(f"Unexpected CSV path for {provider_name}")
        if not os.path.isfile(expected_path):
            raise ValueError(f"Planned CSV is missing: {expected_path}")
        if entry["sha256"] != _sha256(expected_path):
            raise ValueError(f"Planned CSV changed: {expected_path}")


def _load_progress(path: str, plan_sha256: str, operations_sha256: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    progress = _read_json_object(path)
    if progress["plan_sha256"] != plan_sha256:
        raise ValueError("Progress file belongs to a different correction plan")
    if progress["operations_sha256"] != operations_sha256:
        raise ValueError("Progress file contains a different operation set")
    completed = progress["completed_operations"]
    if not isinstance(completed, list) or not all(
        isinstance(operation_id, str) for operation_id in completed
    ):
        raise ValueError("Invalid completed_operations in progress file")
    return set(cast(list[str], completed))


def _save_progress(
    path: str,
    plan_sha256: str,
    operations_sha256: str,
    completed: set[str],
) -> None:
    _write_json(
        path,
        {
            "plan_sha256": plan_sha256,
            "operations_sha256": operations_sha256,
            "completed_operations": sorted(completed),
        },
    )


def _current_contexts(
    config: AuditConfig, role_uris: set[str], workers: int
) -> dict[str, tuple[str, ...]]:
    if not role_uris:
        return {}
    br_files = _data_files(os.path.join(config.rdf_dir, "br"), config.zip_output)
    return _scan_contexts(br_files, frozenset(role_uris), workers)


def _capture_preconditions(
    operation: dict[str, object],
    config: AuditConfig,
    contexts: Mapping[str, tuple[str, ...]],
    links: set[tuple[str, str]],
    completed_deleted_roles: set[str],
    workers: int,
) -> dict[str, object]:
    work_summary = cast(dict[str, object], operation["work"])
    br_uri = cast(str, work_summary["br"])
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    work_entities = load_available_entities({br_uri}, locator, workers)
    if br_uri not in work_entities:
        raise RuntimeError(f"Stale plan: bibliographic resource is missing: {br_uri}")
    work_entity = work_entities[br_uri]
    work = WorkRecord(
        br_uri,
        tuple(_ids(work_entity, IS_DOCUMENT_CONTEXT_FOR)),
        tuple(_ids(work_entity, HAS_IDENTIFIER)),
    )
    role_entities = load_available_entities(set(work.role_uris), locator, workers)
    missing = tuple(sorted(set(work.role_uris) - role_entities.keys()))
    identifier_entities = load_available_entities(
        set(work.identifier_uris), locator, workers
    )
    raw_entities = {br_uri: work_entity, **role_entities, **identifier_entities}
    planned_preconditions = cast(dict[str, object], operation["preconditions"])
    owned_provenance = cast(
        dict[str, str], planned_preconditions["owned_missing_provenance"]
    )
    provenance = load_provenance_statuses(set(owned_provenance), locator, workers)
    current = _operation_preconditions(
        work,
        role_entities,
        raw_entities,
        missing,
        contexts,
        links,
        set(owned_provenance),
        provenance,
    )
    current_roles = cast(dict[str, object], current["role_entities"])
    planned_roles = cast(dict[str, object], planned_preconditions["role_entities"])
    if completed_deleted_roles.intersection(current_roles):
        raise RuntimeError(f"Stale plan: a deleted agent role reappeared for {br_uri}")
    current["role_entities"] = {
        uri: entity
        for uri, entity in current_roles.items()
        if uri not in completed_deleted_roles
    }
    expected = dict(planned_preconditions)
    expected["role_entities"] = {
        uri: entity
        for uri, entity in planned_roles.items()
        if uri not in completed_deleted_roles
    }
    expected["dangling_ar_references"] = sorted(
        set(cast(list[str], expected["dangling_ar_references"]))
        | completed_deleted_roles.intersection(work.role_uris)
    )
    current_edges = cast(list[list[str]], current["has_next_edges"])
    planned_edges = cast(list[list[str]], expected["has_next_edges"])
    new_edges_to_deleted_roles = {
        tuple(edge)
        for edge in current_edges
        if completed_deleted_roles.intersection(edge)
    } - {tuple(edge) for edge in planned_edges}
    if new_edges_to_deleted_roles:
        raise RuntimeError(
            f"Stale plan: role links changed for {br_uri}: "
            f"{sorted(new_edges_to_deleted_roles)}"
        )
    current["has_next_edges"] = [
        edge for edge in current_edges if not completed_deleted_roles.intersection(edge)
    ]
    expected["has_next_edges"] = [
        edge for edge in planned_edges if not completed_deleted_roles.intersection(edge)
    ]
    if current != expected:
        raise RuntimeError(f"Stale plan: local RDF state changed for {br_uri}")
    return current


def _import_exact_entities(
    editor: MetaEditor,
    g_set: GraphSet,
    uris: set[str],
    locator: EntityFileLocator,
) -> None:
    uris_by_path: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        uris_by_path[locator.path(uri)].add(uri)
    for path, path_uris in sorted(uris_by_path.items()):
        if not os.path.exists(path):
            continue
        graph = editor.reader.load(path)
        if graph is None:
            continue
        merged = TripleLite()
        for context in from_rdflib(graph):
            for triple in context.triples((None, None, None)):
                merged.add(triple)
        for uri in sorted(path_uris):
            preexisting = merged.subgraph(uri)
            if "/br/" in uri:
                g_set.add_br(editor.resp_agent, res=uri, preexisting_graph=preexisting)
            elif "/ar/" in uri:
                g_set.add_ar(editor.resp_agent, res=uri, preexisting_graph=preexisting)
            else:
                raise ValueError(f"Unsupported entity in correction operation: {uri}")


def _bibliographic_resource(g_set: GraphSet, uri: str) -> BibliographicResource:
    entity = g_set.get_entity(uri)
    if not isinstance(entity, BibliographicResource):
        raise ValueError(f"Bibliographic resource not imported: {uri}")
    return entity


def _apply_operation(
    editor: MetaEditor,
    operation: dict[str, object],
    locator: EntityFileLocator,
) -> None:
    work = cast(dict[str, object], operation["work"])
    br_uri = cast(str, work["br"])
    actions = cast(dict[str, object], operation["actions"])
    delete_existing = set(cast(list[str], actions["delete_existing_ars"]))
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=editor.supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    _import_exact_entities(editor, g_set, {br_uri, *delete_existing}, locator)
    br = _bibliographic_resource(g_set, br_uri)
    for role_uri in cast(list[str], actions["remove_role_references"]):
        br.g.remove(
            (
                br.res,
                GraphEntity.iri_is_document_context_for,
                RDFTerm("uri", role_uri),
            )
        )
    for role_uri in sorted(delete_existing):
        role = g_set.get_entity(role_uri)
        if role is None:
            raise ValueError(f"Agent role not imported: {role_uri}")
        role.mark_as_to_be_deleted()
    for role_uri in cast(list[str], actions["invalidate_missing_ars"]):
        g_set.add_ar(editor.resp_agent, res=role_uri).mark_as_to_be_deleted()
    editor.save(g_set, editor.supplier_prefix)


def _write_reindex_sentinel(path: str, plan_path: str) -> None:
    with open(path, "w", encoding="utf-8") as stream:
        stream.write(
            f"{plan_path} changed local RDF files.\nRe-index the data and provenance "
            "triplestores from the RDF files, then delete this file. Run Meta for "
            "the Crossref and DataCite CSV directories only after this plan is "
            "complete.\n"
        )


def execute_plan(
    config_path: str,
    plan_path: str,
    resp_agent: str,
    progress_path: str,
    execution_report_path: str,
    workers: int,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    config_path = os.path.abspath(config_path)
    plan_path = os.path.abspath(plan_path)
    plan = _read_json_object(plan_path)
    operations = _plan_operations(plan)
    if plan["complete"] is not True or plan["executable"] is not True:
        raise ValueError("The correction plan is not executable")
    if plan["config_sha256"] != _sha256(config_path):
        raise ValueError("The Meta configuration changed after plan generation")
    if any(cast(list[object], operation["blockers"]) for operation in operations):
        raise ValueError("An executable correction plan cannot contain blockers")
    _verify_csv_files(plan)
    plan_sha256 = _sha256(plan_path)
    operations_sha256 = cast(str, plan["operations_sha256"])
    completed = _load_progress(progress_path, plan_sha256, operations_sha256)
    operation_ids = {cast(str, operation["operation_id"]) for operation in operations}
    unknown_completed = completed - operation_ids
    if unknown_completed:
        raise ValueError(
            f"Progress file contains unknown operations: {sorted(unknown_completed)}"
        )
    sentinel_path = os.path.join(os.path.dirname(plan_path), REINDEX_SENTINEL_FILENAME)
    if os.path.exists(sentinel_path) and not os.path.exists(progress_path):
        raise RuntimeError(
            f"{sentinel_path} exists. Re-index the triplestores and remove the "
            "sentinel before starting another correction plan."
        )
    pending = [
        operation
        for operation in operations
        if cast(str, operation["operation_id"]) not in completed
    ]
    completed_deleted_roles = {
        role_uri
        for operation in operations
        if cast(str, operation["operation_id"]) in completed
        for role_uri in cast(
            list[str],
            cast(dict[str, object], operation["actions"])["delete_existing_ars"],
        )
    }
    pending_role_uris = {
        role_uri
        for operation in pending
        for role_uri in cast(
            list[str],
            cast(dict[str, object], operation["preconditions"])["role_references"],
        )
    }
    config = load_audit_config(config_path)
    contexts = _current_contexts(config, pending_role_uris, workers)
    links = scan_role_links(config, pending_role_uris, workers)
    for operation in pending:
        _capture_preconditions(
            operation,
            config,
            contexts,
            links,
            completed_deleted_roles,
            workers,
        )
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    attempted = 0
    if pending:
        _save_progress(progress_path, plan_sha256, operations_sha256, completed)
        editor = MetaEditor(config_path, resp_agent, save_queries=True)
        editor.rdf_files_only = True
        try:
            with create_progress() as progress:
                task = progress.add_task(
                    "Applying correction plan", total=len(operations)
                )
                progress.advance(task, len(completed))
                for operation in pending:
                    if _stop_requested:
                        break
                    attempted += 1
                    _apply_operation(editor, operation, locator)
                    completed.add(cast(str, operation["operation_id"]))
                    _save_progress(
                        progress_path, plan_sha256, operations_sha256, completed
                    )
                    progress.advance(task)
        finally:
            if attempted:
                _write_reindex_sentinel(sentinel_path, plan_path)
    complete = len(completed) == len(operations) and not _stop_requested
    execution_report: dict[str, object] = {
        "plan": plan_path,
        "plan_sha256": plan_sha256,
        "operations_sha256": operations_sha256,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "complete": complete,
        "planned_operations": len(operations),
        "completed_operations": sorted(completed),
        "reindex_sentinel": sentinel_path if os.path.exists(sentinel_path) else None,
    }
    _write_json(execution_report_path, execution_report)
    if complete and os.path.exists(progress_path):
        os.remove(progress_path)
    return execution_report


def _validate_uri(uri: str) -> None:
    if not uri.startswith(("http://", "https://")) or any(
        character in uri for character in "<> \t\r\n"
    ):
        raise ValueError(f"Invalid URI: {uri}")


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Find bibliographic resources that reference missing agent roles, "
            "remove all their contributor roles, and prepare Crossref or DataCite "
            "CSV input for Meta."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta YAML config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run", action="store_true", help="Generate a plan without changing RDF"
    )
    mode.add_argument("--execute", metavar="PLAN", help="Execute a complete plan")
    parser.add_argument("--report-file", help="Dry-run JSON plan path")
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
    if args.workers < 1:
        parser.error("--workers must be positive")
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if args.dry_run:
        if not args.report_file or not args.csv_output_dir or not args.mailto:
            parser.error(
                "--report-file, --csv-output-dir, and --mailto are required with --dry-run"
            )
        cache_path = args.cache_file or f"{args.report_file}.cache.sqlite"
        report = analyze_dangling_ars(
            config_path=args.config,
            report_path=args.report_file,
            csv_output_dir=args.csv_output_dir,
            cache_path=cache_path,
            mailto=args.mailto,
            workers=args.workers,
            refresh_cache=args.refresh_cache,
        )
        summary = cast(dict[str, object], report["summary"])
        console.print(
            f"Plan written to [cyan]{os.path.abspath(args.report_file)}[/cyan]. "
            f"Affected BRs: [cyan]{summary['affected_brs']}[/cyan]; dangling AR "
            f"references: [cyan]{summary['dangling_ar_references']}[/cyan]."
        )
        if report["executable"] is not True:
            raise SystemExit(1)
        return

    if not args.resp_agent:
        parser.error("--resp-agent is required with --execute")
    _validate_uri(args.resp_agent)
    plan_path = os.path.abspath(cast(str, args.execute))
    progress_path = os.path.abspath(args.progress_file or f"{plan_path}.progress.json")
    execution_report_path = os.path.abspath(
        args.execution_report or f"{plan_path}.execution.json"
    )
    result = execute_plan(
        config_path=args.config,
        plan_path=plan_path,
        resp_agent=args.resp_agent,
        progress_path=progress_path,
        execution_report_path=execution_report_path,
        workers=args.workers,
    )
    console.print(
        f"Execution report written to [cyan]{execution_report_path}[/cyan]. "
        f"Completed operations: "
        f"[cyan]{len(cast(list[str], result['completed_operations']))}[/cyan]."
    )
    if result["complete"] is not True:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
