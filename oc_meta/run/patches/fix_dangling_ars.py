# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import signal
from collections import Counter, defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, cast

import orjson
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic.agent_role import AgentRole
from oc_ocdm.graph.entities.bibliographic.bibliographic_resource import (
    BibliographicResource,
)
from oc_ocdm.graph.entities.bibliographic.responsible_agent import ResponsibleAgent
from oc_ocdm.graph.entities.identifier import Identifier
from oc_ocdm.graph.graph_entity import GraphEntity
from rich_argparse import RichHelpFormatter
from triplelite import RDFTerm

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.agent_matching import PersonName, name_score
from oc_meta.lib.agent_metadata import (
    AgentMetadata,
    AgentMetadataClient,
    ApiCache,
    WorkMetadata,
    agents_for_role,
    normalize_orcid,
)
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.rdf_patch import (
    DATACITE_PREFIX,
    FAMILY_NAME,
    FOAF_NAME,
    GIVEN_NAME,
    HAS_IDENTIFIER,
    HAS_LITERAL_VALUE,
    HAS_NEXT,
    IS_DOCUMENT_CONTEXT_FOR,
    IS_HELD_BY,
    PROV_SPECIALIZATION_OF,
    ROLE_MAP,
    USES_IDENTIFIER_SCHEME,
    WITH_ROLE,
    AuditConfig,
    EntityFileLocator,
    agent_role as _agent_role,
    batches as _batches,
    data_files as _data_files,
    ensure_parent as _ensure_parent,
    first as _first,
    identifier as _identifier,
    ids as _ids,
    literals as _literals,
    load_audit_config,
    load_available_entities,
    load_entities as _load_entities,
    load_progress as _load_progress,
    provenance_path as _provenance_path,
    read_json_object as _read_json_object,
    responsible_agent as _responsible_agent,
    save_progress as _save_progress,
    sha256 as _sha256,
    snapshot_number as _snapshot_number,
    write_json as _write_json,
)
from oc_meta.run.merge.entities import REINDEX_SENTINEL_FILENAME

PROV_INVALIDATED_AT_TIME = "http://www.w3.org/ns/prov#invalidatedAtTime"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"

SUPPORTED_AGENT_IDENTIFIERS = frozenset({"orcid", "crossref", "ror"})
ROLE_NAMES = ("author", "editor", "publisher")
CONFIRMED_NAME_SCORE = 0.9
PLAN_SCHEMA_VERSION = 1
REVIEW_FIELDS: list[str] = [
    "group_id",
    "br",
    "missing_ars",
    "provider_sources",
    "authors",
    "editors",
    "publishers",
    "reused_ars",
    "created_ars",
    "reassigned_ars",
    "deleted_ars",
    "status",
    "decision",
]

_stop_requested = False


@dataclass(frozen=True, slots=True)
class WorkRecord:
    uri: str
    role_uris: tuple[str, ...]
    identifier_uris: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RoleRecord:
    uri: str
    roles: tuple[str, ...]
    holders: tuple[str, ...]
    next_uris: tuple[str, ...]

    @property
    def role(self) -> str:
        return ROLE_MAP[self.roles[0]] if len(self.roles) == 1 else "unknown"


@dataclass(frozen=True, slots=True)
class IdentifierRecord:
    uri: str
    scheme: str
    value: str


@dataclass(frozen=True, slots=True)
class AgentRecord:
    uri: str
    name: PersonName
    identifiers: tuple[IdentifierRecord, ...]


@dataclass(frozen=True, slots=True)
class IdentityEntry:
    identifier_uris: tuple[str, ...]
    ra_uris: tuple[str, ...]


class WorkProviderClient(Protocol):
    def all_work_sources(
        self, doi: str, openalex_id: str = ""
    ) -> list[WorkMetadata]: ...


def _handle_signal(signum: int, frame: object) -> None:
    del signum, frame
    global _stop_requested
    _stop_requested = True


def _object_sha256(value: object) -> str:
    return hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()


def _scan_entity_uri_batch(paths: list[str]) -> set[str]:
    return {uri for path in paths for uri in _load_entities(path)}


def scan_entity_uris(files: list[str], workers: int) -> set[str]:
    result = set()
    batches = _batches(files, 24)
    with create_progress() as progress:
        task = progress.add_task("Indexing agent roles", total=len(files))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch, partial in zip(
                batches, executor.map(_scan_entity_uri_batch, batches)
            ):
                result.update(partial)
                progress.advance(task, len(batch))
    return result


def _scan_dangling_work_batch(
    paths: list[str], existing_roles: frozenset[str]
) -> tuple[dict[str, WorkRecord], dict[str, tuple[str, ...]]]:
    works = {}
    missing_by_work = {}
    for path in paths:
        for uri, entity in _load_entities(path).items():
            role_uris = tuple(_ids(entity, IS_DOCUMENT_CONTEXT_FOR))
            missing = tuple(sorted(set(role_uris) - existing_roles))
            if not missing:
                continue
            works[uri] = WorkRecord(uri, role_uris, tuple(_ids(entity, HAS_IDENTIFIER)))
            missing_by_work[uri] = missing
    return works, missing_by_work


def _scan_role_context_batch(
    paths: list[str], target_roles: frozenset[str]
) -> dict[str, list[str]]:
    contexts: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        for uri, entity in _load_entities(path).items():
            for role_uri in _ids(entity, IS_DOCUMENT_CONTEXT_FOR):
                if role_uri in target_roles:
                    contexts[role_uri].append(uri)
    return contexts


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
    with create_progress() as progress:
        task = progress.add_task(
            "Finding dangling role references", total=len(br_files)
        )
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch, (partial_works, partial_missing) in zip(
                batches,
                executor.map(
                    lambda paths: _scan_dangling_work_batch(paths, existing_roles),
                    batches,
                ),
            ):
                works.update(partial_works)
                missing_by_work.update(partial_missing)
                progress.advance(task, len(batch))
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    target_roles = frozenset(
        role_uri for work in works.values() for role_uri in work.role_uris
    )
    role_entities = load_available_entities(
        set(target_roles.intersection(existing_roles)), locator, workers
    )
    role_contexts: dict[str, list[str]] = defaultdict(list)
    batches = _batches(br_files, 24)
    with create_progress() as progress:
        task = progress.add_task("Checking role contexts", total=len(br_files))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch, partial in zip(
                batches,
                executor.map(
                    lambda paths: _scan_role_context_batch(paths, target_roles),
                    batches,
                ),
            ):
                for role_uri, work_uris in partial.items():
                    role_contexts[role_uri].extend(work_uris)
                progress.advance(task, len(batch))
    contexts = {
        role_uri: tuple(sorted(role_contexts[role_uri])) for role_uri in target_roles
    }
    return works, role_entities, missing_by_work, contexts


def _identifier_record(uri: str, entity: dict[str, object]) -> IdentifierRecord | None:
    scheme_uri = _first(_ids(entity, USES_IDENTIFIER_SCHEME))
    value = _first(_literals(entity, HAS_LITERAL_VALUE))
    if not scheme_uri or not value:
        return None
    scheme = (
        scheme_uri[len(DATACITE_PREFIX) :]
        if scheme_uri.startswith(DATACITE_PREFIX)
        else scheme_uri
    )
    return IdentifierRecord(uri, scheme, value)


def _role_record(uri: str, entity: dict[str, object]) -> RoleRecord:
    return RoleRecord(
        uri,
        tuple(_ids(entity, WITH_ROLE)),
        tuple(_ids(entity, IS_HELD_BY)),
        tuple(_ids(entity, HAS_NEXT)),
    )


def _agent_record(
    uri: str,
    entity: dict[str, object],
    identifiers: dict[str, IdentifierRecord],
) -> AgentRecord:
    return AgentRecord(
        uri,
        PersonName(
            name=_first(_literals(entity, FOAF_NAME)),
            given=_first(_literals(entity, GIVEN_NAME)),
            family=_first(_literals(entity, FAMILY_NAME)),
        ),
        tuple(
            identifiers[identifier_uri]
            for identifier_uri in _ids(entity, HAS_IDENTIFIER)
            if identifier_uri in identifiers
        ),
    )


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
            "invalidated" if _literals(latest, PROV_INVALIDATED_AT_TIME) else "active"
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
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for partial in executor.map(_provenance_status_batch, _batches(tasks, 24)):
            statuses.update(partial)
    return {uri: statuses[uri] if uri in statuses else "missing" for uri in uris}


def _local_context(
    works: dict[str, WorkRecord],
    role_entities: dict[str, dict[str, object]],
    missing_by_work: dict[str, tuple[str, ...]],
    locator: EntityFileLocator,
    workers: int,
) -> tuple[
    dict[str, RoleRecord],
    dict[str, AgentRecord],
    dict[str, IdentifierRecord],
    dict[str, dict[str, object]],
    dict[str, str],
]:
    affected_role_uris = {uri for work in works.values() for uri in work.role_uris}
    selected_role_entities = {
        uri: role_entities[uri] for uri in affected_role_uris if uri in role_entities
    }
    roles = {
        uri: _role_record(uri, entity) for uri, entity in selected_role_entities.items()
    }
    ra_uris = {holder for role in roles.values() for holder in role.holders}
    work_entities = load_available_entities(set(works), locator, workers)
    ra_entities = load_available_entities(ra_uris, locator, workers)
    identifier_uris = {
        identifier_uri
        for work in works.values()
        for identifier_uri in work.identifier_uris
    }
    identifier_uris.update(
        identifier_uri
        for entity in ra_entities.values()
        for identifier_uri in _ids(entity, HAS_IDENTIFIER)
    )
    identifier_entities = load_available_entities(identifier_uris, locator, workers)
    identifiers = {
        uri: record
        for uri, entity in identifier_entities.items()
        if (record := _identifier_record(uri, entity)) is not None
    }
    agents = {
        uri: _agent_record(uri, entity, identifiers)
        for uri, entity in ra_entities.items()
    }
    raw_entities = dict(work_entities)
    raw_entities.update(selected_role_entities)
    raw_entities.update(ra_entities)
    raw_entities.update(identifier_entities)
    missing_uris = {uri for uris in missing_by_work.values() for uri in uris}
    provenance = load_provenance_statuses(missing_uris, locator, workers)
    return roles, agents, identifiers, raw_entities, provenance


def _work_identifiers(
    work: WorkRecord, identifiers: dict[str, IdentifierRecord]
) -> dict[str, str]:
    return {
        identifiers[uri].scheme: identifiers[uri].value
        for uri in work.identifier_uris
        if uri in identifiers
    }


def _structural_anomalies(
    work: WorkRecord,
    roles: dict[str, RoleRecord],
    contexts: dict[str, tuple[str, ...]],
) -> list[str]:
    anomalies = []
    existing = {uri for uri in work.role_uris if uri in roles}
    for role_uri in sorted(existing):
        role = roles[role_uri]
        if len(contexts[role_uri]) != 1:
            anomalies.append(f"multiple_contexts:{role_uri}")
        if len(role.roles) != 1 or role.roles[0] not in ROLE_MAP:
            anomalies.append(f"invalid_role:{role_uri}")
        if len(role.holders) != 1:
            anomalies.append(f"invalid_holder:{role_uri}")
        if len(role.next_uris) > 1:
            anomalies.append(f"fork:{role_uri}")
        if role.uri in role.next_uris:
            anomalies.append(f"self_loop:{role_uri}")
        for next_uri in role.next_uris:
            if next_uri not in existing:
                if next_uri not in work.role_uris:
                    anomalies.append(f"cross_context_link:{role_uri}")
                continue
            next_role = roles[next_uri]
            if role.role != next_role.role:
                anomalies.append(f"cross_role_link:{role_uri}")
    predecessor_counts = Counter(
        next_uri
        for role_uri in existing
        for next_uri in set(roles[role_uri].next_uris)
        if next_uri in existing
    )
    anomalies.extend(
        f"multiple_predecessors:{role_uri}"
        for role_uri, count in predecessor_counts.items()
        if count > 1
    )
    for role_name in ROLE_NAMES:
        role_group = {
            uri: roles[uri] for uri in existing if roles[uri].role == role_name
        }
        for start_uri in role_group:
            seen = set()
            current = start_uri
            while current in role_group and current not in seen:
                seen.add(current)
                next_uris = role_group[current].next_uris
                current = next_uris[0] if next_uris else ""
            if current in seen:
                anomalies.append(f"cycle:{role_name}")
                break
    return sorted(set(anomalies))


def _ordered_roles(roles: list[RoleRecord]) -> list[RoleRecord]:
    if not roles:
        return []
    by_uri = {role.uri: role for role in roles}
    targets = {
        next_uri for role in roles for next_uri in role.next_uris if next_uri in by_uri
    }
    heads = sorted(uri for uri in by_uri if uri not in targets)
    ordered = []
    visited = set()
    for head in heads:
        current = head
        while current in by_uri and current not in visited:
            visited.add(current)
            ordered.append(by_uri[current])
            next_uris = [uri for uri in by_uri[current].next_uris if uri in by_uri]
            current = next_uris[0] if next_uris else ""
    ordered.extend(by_uri[uri] for uri in sorted(by_uri.keys() - visited))
    return ordered


def _normalized_identifier(scheme: str, value: str) -> tuple[str, str]:
    normalized_scheme = scheme.casefold()
    normalized_value = value.strip()
    if normalized_scheme == "orcid":
        normalized_value = normalize_orcid(normalized_value)
    elif normalized_scheme in {"crossref", "ror"}:
        normalized_value = normalized_value.rstrip("/").rsplit("/", 1)[-1]
        if normalized_scheme == "ror":
            normalized_value = normalized_value.casefold()
    return normalized_scheme, normalized_value


def _supported_agent_identifiers(
    agent: AgentMetadata,
) -> tuple[tuple[str, str], ...]:
    return tuple(
        _normalized_identifier(identifier["scheme"], identifier["value"])
        for identifier in agent["identifiers"]
        if identifier["scheme"].casefold() in SUPPORTED_AGENT_IDENTIFIERS
    )


def select_provider_targets(
    works: list[WorkMetadata],
) -> tuple[dict[str, list[AgentMetadata]], dict[str, str]]:
    targets = {}
    sources = {}
    for role_name in ROLE_NAMES:
        selected: list[AgentMetadata] = []
        source = ""
        for work in works:
            candidates = agents_for_role(work, role_name)
            if candidates:
                selected = candidates
                source = work["source"]
                break
        targets[role_name] = selected
        sources[role_name] = source
    return targets, sources


def _scan_identity_identifier_batch(
    paths: list[str], targets: frozenset[tuple[str, str]]
) -> dict[tuple[str, str], list[str]]:
    result: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in paths:
        for uri, entity in _load_entities(path).items():
            record = _identifier_record(uri, entity)
            if record is None:
                continue
            key = _normalized_identifier(record.scheme, record.value)
            if key in targets:
                result[key].append(uri)
    return result


def _scan_identity_agent_batch(
    paths: list[str], target_identifiers: frozenset[str]
) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        for uri, entity in _load_entities(path).items():
            for identifier_uri in _ids(entity, HAS_IDENTIFIER):
                if identifier_uri in target_identifiers:
                    result[identifier_uri].append(uri)
    return result


def scan_identity_index(
    config: AuditConfig,
    targets: set[tuple[str, str]],
    workers: int,
) -> dict[tuple[str, str], IdentityEntry]:
    if not targets:
        return {}
    target_set = frozenset(targets)
    identifier_files = _data_files(
        os.path.join(config.rdf_dir, "id"), config.zip_output
    )
    identifier_uris: dict[tuple[str, str], list[str]] = defaultdict(list)
    batches = _batches(identifier_files, 24)
    with create_progress() as progress:
        task = progress.add_task("Resolving external identifiers", total=len(batches))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for partial in executor.map(
                lambda paths: _scan_identity_identifier_batch(paths, target_set),
                batches,
            ):
                for key, uris in partial.items():
                    identifier_uris[key].extend(uris)
                progress.advance(task)

    all_identifier_uris = frozenset(
        uri for uris in identifier_uris.values() for uri in uris
    )
    ra_files = _data_files(os.path.join(config.rdf_dir, "ra"), config.zip_output)
    ras_by_identifier: dict[str, list[str]] = defaultdict(list)
    batches = _batches(ra_files, 24)
    with create_progress() as progress:
        task = progress.add_task("Finding identifier holders", total=len(batches))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for partial in executor.map(
                lambda paths: _scan_identity_agent_batch(paths, all_identifier_uris),
                batches,
            ):
                for identifier_uri, ra_uris in partial.items():
                    ras_by_identifier[identifier_uri].extend(ra_uris)
                progress.advance(task)

    return {
        key: IdentityEntry(
            tuple(sorted(set(identifier_uris[key]))),
            tuple(
                sorted(
                    {
                        ra_uri
                        for identifier_uri in identifier_uris[key]
                        for ra_uri in ras_by_identifier[identifier_uri]
                    }
                )
            ),
        )
        for key in targets
    }


def _agent_plan(agent: AgentMetadata) -> dict[str, object]:
    return {
        "family": agent["family"],
        "given": agent["given"],
        "name": agent["name"],
        "identifiers": [
            {"scheme": scheme, "value": value}
            for scheme, value in _supported_agent_identifiers(agent)
        ],
    }


def _agent_name(agent: AgentMetadata) -> PersonName:
    return PersonName(name=agent["name"], given=agent["given"], family=agent["family"])


def _has_conflicting_identifier(local: AgentRecord, external: AgentMetadata) -> bool:
    external_by_scheme: dict[str, set[str]] = defaultdict(set)
    for scheme, value in _supported_agent_identifiers(external):
        external_by_scheme[scheme].add(value)
    local_by_scheme: dict[str, set[str]] = defaultdict(set)
    for identifier in local.identifiers:
        scheme, value = _normalized_identifier(identifier.scheme, identifier.value)
        if scheme in SUPPORTED_AGENT_IDENTIFIERS:
            local_by_scheme[scheme].add(value)
    return any(
        scheme in local_by_scheme
        and local_by_scheme[scheme].isdisjoint(external_values)
        for scheme, external_values in external_by_scheme.items()
    )


def _identity_resolution(
    agent: AgentMetadata,
    identity_index: dict[tuple[str, str], IdentityEntry],
) -> tuple[str, list[dict[str, str]], str]:
    identifiers = _supported_agent_identifiers(agent)
    ra_uris = {
        ra_uri
        for identifier in identifiers
        for ra_uri in identity_index[identifier].ra_uris
    }
    if len(ra_uris) > 1:
        return "", [], "ambiguous_identifier"
    if ra_uris:
        return next(iter(ra_uris)), [], ""
    orphan_identifiers = []
    for scheme, value in identifiers:
        entry = identity_index[(scheme, value)]
        if len(entry.identifier_uris) > 1:
            return "", [], "ambiguous_identifier"
        orphan_identifiers.append(
            {
                "scheme": scheme,
                "value": value,
                "identifier_uri": (
                    entry.identifier_uris[0] if entry.identifier_uris else ""
                ),
            }
        )
    return "", orphan_identifiers, ""


def _reconcile_role(
    role_name: str,
    current_roles: list[RoleRecord],
    external_agents: list[AgentMetadata],
    agents: dict[str, AgentRecord],
    identity_index: dict[tuple[str, str], IdentityEntry],
    source: str,
) -> tuple[dict[str, object], str]:
    ordered = _ordered_roles(current_roles)
    available = set(range(len(ordered)))
    targets = []
    for position, external in enumerate(external_agents):
        exact_ra, orphan_identifiers, error = _identity_resolution(
            external, identity_index
        )
        if error:
            return {}, error
        resolution = "identifier" if exact_ra else "new"
        selected_index: int | None = None
        if exact_ra:
            matching_roles = [
                index
                for index in sorted(available)
                if ordered[index].holders[0] == exact_ra
            ]
            if matching_roles:
                selected_index = matching_roles[0]
        else:
            scored = sorted(
                (
                    name_score(
                        agents[ordered[index].holders[0]].name, _agent_name(external)
                    ),
                    index,
                )
                for index in available
                if not _has_conflicting_identifier(
                    agents[ordered[index].holders[0]], external
                )
            )
            if scored and scored[-1][0] >= CONFIRMED_NAME_SCORE:
                top_score = scored[-1][0]
                top = [index for score, index in scored if score == top_score]
                if len(top) == 1:
                    selected_index = top[0]
                    exact_ra = ordered[selected_index].holders[0]
                    orphan_identifiers = []
                    resolution = "name"
        selected_role = ordered[selected_index] if selected_index is not None else None
        if selected_index is not None:
            available.remove(selected_index)
        targets.append(
            {
                "position": position,
                "agent": _agent_plan(external),
                "source": source,
                "ar": selected_role.uri if selected_role is not None else "",
                "old_ra": (
                    selected_role.holders[0] if selected_role is not None else ""
                ),
                "ra": exact_ra,
                "create_ra": not bool(exact_ra),
                "resolution": resolution,
                "identifier_resolutions": orphan_identifiers,
            }
        )

    unassigned_targets = [target for target in targets if not target["ar"]]
    remaining_roles = [ordered[index] for index in sorted(available)]
    for target, role in zip(unassigned_targets, remaining_roles):
        target["ar"] = role.uri
        target["old_ra"] = role.holders[0]
        available.remove(ordered.index(role))
    deleted = [ordered[index].uri for index in sorted(available)]
    return {
        "role": role_name,
        "source": source,
        "targets": targets,
        "delete_ars": deleted,
    }, ""


def _local_state(
    work: WorkRecord,
    roles: dict[str, RoleRecord],
    agents: dict[str, AgentRecord],
    raw_entities: dict[str, dict[str, object]],
    missing: tuple[str, ...],
    provenance: dict[str, str],
) -> dict[str, object]:
    entity_uris = {work.uri}
    for role_uri in work.role_uris:
        if role_uri not in roles:
            continue
        entity_uris.add(role_uri)
        for holder in roles[role_uri].holders:
            if holder not in agents:
                continue
            entity_uris.add(holder)
            entity_uris.update(
                identifier.uri for identifier in agents[holder].identifiers
            )
    entity_uris.update(work.identifier_uris)
    return {
        "entities": {
            uri: raw_entities[uri] for uri in sorted(entity_uris) if uri in raw_entities
        },
        "missing_ars": list(missing),
        "missing_ar_provenance": {uri: provenance[uri] for uri in missing},
    }


def _repair_counts(repair: dict[str, object]) -> dict[str, int]:
    counts = Counter()
    role_plans = repair["role_plans"]
    if not isinstance(role_plans, list):
        raise ValueError("role_plans must be a list")
    for raw_role_plan in role_plans:
        role_plan = cast(dict[str, object], raw_role_plan)
        targets = cast(list[dict[str, object]], role_plan["targets"])
        for target in targets:
            if not target["ar"]:
                counts["created_ars"] += 1
            elif target["create_ra"] or target["old_ra"] != target["ra"]:
                counts["reassigned_ars"] += 1
            else:
                counts["reused_ars"] += 1
        counts["deleted_ars"] += len(cast(list[str], role_plan["delete_ars"]))
    return {
        "reused_ars": counts["reused_ars"],
        "created_ars": counts["created_ars"],
        "reassigned_ars": counts["reassigned_ars"],
        "deleted_ars": counts["deleted_ars"],
    }


def _finalize_repair(repair: dict[str, object]) -> dict[str, object]:
    repair["counts"] = _repair_counts(repair)
    repair["group_id"] = _object_sha256(repair)[:20]
    return repair


def _build_repair(
    draft: dict[str, object],
    roles: dict[str, RoleRecord],
    agents: dict[str, AgentRecord],
    identity_index: dict[tuple[str, str], IdentityEntry],
) -> dict[str, object]:
    status = cast(str, draft["status"])
    if status != "ready":
        draft["role_plans"] = []
        draft.pop("work_record")
        if "targets" in draft:
            draft.pop("targets")
        return _finalize_repair(draft)
    work = cast(WorkRecord, draft["work_record"])
    targets = cast(dict[str, list[AgentMetadata]], draft.pop("targets"))
    sources = cast(dict[str, str], draft["role_sources"])
    role_plans = []
    for role_name in ROLE_NAMES:
        current = [
            roles[uri]
            for uri in work.role_uris
            if uri in roles and roles[uri].role == role_name
        ]
        role_plan, error = _reconcile_role(
            role_name,
            current,
            targets[role_name],
            agents,
            identity_index,
            sources[role_name],
        )
        if error:
            draft["status"] = "blocked"
            draft["reason"] = error
            draft["role_plans"] = []
            draft.pop("work_record")
            return _finalize_repair(draft)
        role_plans.append(role_plan)
    draft["role_plans"] = role_plans
    draft.pop("work_record")
    return _finalize_repair(draft)


def _repair_payload(repair: dict[str, object]) -> dict[str, object]:
    payload = dict(repair)
    payload.pop("group_id")
    return payload


def _verify_repair_id(repair: dict[str, object]) -> None:
    group_id = repair["group_id"]
    if not isinstance(group_id, str):
        raise ValueError("Repair group_id must be a string")
    if _object_sha256(_repair_payload(repair))[:20] != group_id:
        raise ValueError(f"Correction plan has modified repair group {group_id}")


def _target_identifier_keys(
    targets: dict[str, list[AgentMetadata]],
) -> set[tuple[str, str]]:
    return {
        identifier
        for role_targets in targets.values()
        for agent in role_targets
        for identifier in _supported_agent_identifiers(agent)
    }


def build_repair_plan(
    works: dict[str, WorkRecord],
    roles: dict[str, RoleRecord],
    agents: dict[str, AgentRecord],
    identifiers: dict[str, IdentifierRecord],
    raw_entities: dict[str, dict[str, object]],
    missing_by_work: dict[str, tuple[str, ...]],
    contexts: dict[str, tuple[str, ...]],
    provenance: dict[str, str],
    provider: WorkProviderClient,
    config: AuditConfig,
    workers: int,
) -> list[dict[str, object]]:
    drafts = []
    identity_targets: set[tuple[str, str]] = set()
    with create_progress() as progress:
        task = progress.add_task("Querying work metadata", total=len(works))
        for br_uri in sorted(works):
            if _stop_requested:
                break
            work = works[br_uri]
            anomalies = _structural_anomalies(work, roles, contexts)
            for missing_uri in missing_by_work[br_uri]:
                if len(contexts[missing_uri]) != 1:
                    anomalies.append(f"multiple_contexts:{missing_uri}")
            for role_uri in work.role_uris:
                if role_uri in roles and any(
                    holder not in agents for holder in roles[role_uri].holders
                ):
                    anomalies.append(f"missing_agent:{role_uri}")
            work_identifiers = _work_identifiers(work, identifiers)
            doi = work_identifiers["doi"] if "doi" in work_identifiers else ""
            openalex_id = (
                work_identifiers["openalex"] if "openalex" in work_identifiers else ""
            )
            provider_works = (
                [] if anomalies else provider.all_work_sources(doi, openalex_id)
            )
            targets, role_sources = select_provider_targets(provider_works)
            status = "ready"
            reason = ""
            if anomalies:
                status = "blocked"
                reason = "structural_anomaly"
            elif not provider_works:
                status = "blocked"
                reason = "no_provider_data"
            if status == "ready":
                identity_targets.update(_target_identifier_keys(targets))
            target_counts = {
                role_name: len(targets[role_name]) for role_name in ROLE_NAMES
            }
            drafts.append(
                {
                    "br": br_uri,
                    "missing_ars": list(missing_by_work[br_uri]),
                    "missing_ar_provenance": {
                        uri: provenance[uri] for uri in missing_by_work[br_uri]
                    },
                    "provider_sources": [
                        work_metadata["source"] for work_metadata in provider_works
                    ],
                    "role_sources": role_sources,
                    "work_identifiers": work_identifiers,
                    "target_counts": target_counts,
                    "anomalies": sorted(set(anomalies)),
                    "status": status,
                    "reason": reason,
                    "local_state": _local_state(
                        work,
                        roles,
                        agents,
                        raw_entities,
                        missing_by_work[br_uri],
                        provenance,
                    ),
                    "work_record": work,
                    "targets": targets,
                }
            )
            progress.advance(task)
    identity_index = scan_identity_index(config, identity_targets, workers)
    return [_build_repair(draft, roles, agents, identity_index) for draft in drafts]


def _review_row(repair: dict[str, object]) -> dict[str, str | int]:
    counts = cast(dict[str, int], repair["counts"])
    target_counts = cast(dict[str, int], repair["target_counts"])
    role_sources = cast(dict[str, str], repair["role_sources"])
    return {
        "group_id": cast(str, repair["group_id"]),
        "br": cast(str, repair["br"]),
        "missing_ars": "; ".join(cast(list[str], repair["missing_ars"])),
        "provider_sources": "; ".join(
            f"{role_name}={role_sources[role_name] or '[empty]'}"
            for role_name in ROLE_NAMES
        ),
        "authors": target_counts["author"],
        "editors": target_counts["editor"],
        "publishers": target_counts["publisher"],
        "reused_ars": counts["reused_ars"],
        "created_ars": counts["created_ars"],
        "reassigned_ars": counts["reassigned_ars"],
        "deleted_ars": counts["deleted_ars"],
        "status": cast(str, repair["status"]),
        "decision": "",
    }


def write_review_file(path: str, repairs: list[dict[str, object]]) -> None:
    _ensure_parent(path)
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(REVIEW_FIELDS))
        writer.writeheader()
        for repair in repairs:
            writer.writerow(_review_row(repair))


def read_review_decisions(
    path: str, repairs: list[dict[str, object]]
) -> list[dict[str, object]]:
    repairs_by_id = {}
    for repair in repairs:
        _verify_repair_id(repair)
        group_id = cast(str, repair["group_id"])
        if group_id in repairs_by_id:
            raise ValueError(f"Repeated repair group: {group_id}")
        repairs_by_id[group_id] = repair
    decisions = {}
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != list(REVIEW_FIELDS):
            raise ValueError(f"Unexpected review CSV header: {reader.fieldnames}")
        for row in reader:
            group_id = row["group_id"]
            if group_id in decisions:
                raise ValueError(f"Repeated review group: {group_id}")
            if group_id not in repairs_by_id:
                raise ValueError(f"Unknown review group: {group_id}")
            expected = {
                field: str(value)
                for field, value in _review_row(repairs_by_id[group_id]).items()
                if field != "decision"
            }
            changed = [
                field
                for field in REVIEW_FIELDS
                if field != "decision" and row[field] != expected[field]
            ]
            if changed:
                raise ValueError(
                    f"Review row {group_id} differs from the plan in: {changed}"
                )
            decision = row["decision"].strip().casefold()
            if decision not in {"", "approve", "reject"}:
                raise ValueError(f"Invalid decision for {group_id}: {row['decision']}")
            if decision == "approve" and repairs_by_id[group_id]["status"] != "ready":
                raise ValueError(f"Blocked repair group cannot be approved: {group_id}")
            decisions[group_id] = decision
    missing = repairs_by_id.keys() - decisions.keys()
    if missing:
        raise ValueError(f"Review CSV is missing groups: {sorted(missing)}")
    return [
        repair
        for group_id, repair in repairs_by_id.items()
        if decisions[group_id] == "approve"
    ]


def analyze_dangling_ars(
    config_path: str,
    report_path: str,
    review_path: str,
    cache_path: str,
    mailto: str,
    workers: int,
    refresh_cache: bool,
    openalex_api_key: str,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    config = load_audit_config(config_path)
    works, role_entities, missing_by_work, contexts = find_dangling_works(
        config, workers
    )
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    roles, agents, identifiers, raw_entities, provenance = _local_context(
        works, role_entities, missing_by_work, locator, workers
    )
    _ensure_parent(cache_path)
    api_cache = ApiCache(cache_path)
    provider = AgentMetadataClient(
        mailto=mailto,
        cache=api_cache,
        refresh_cache=refresh_cache,
        openalex_api_key=openalex_api_key,
    )
    try:
        repairs = build_repair_plan(
            works,
            roles,
            agents,
            identifiers,
            raw_entities,
            missing_by_work,
            contexts,
            provenance,
            provider,
            config,
            workers,
        )
    finally:
        provider.close()
        api_cache.close()
    status_counts = Counter(cast(str, repair["status"]) for repair in repairs)
    reason_counts = Counter(
        cast(str, repair["reason"]) for repair in repairs if cast(str, repair["reason"])
    )
    report: dict[str, object] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "complete": not _stop_requested,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": os.path.abspath(config_path),
        "config_sha256": _sha256(config_path),
        "rdf_dir": config.rdf_dir,
        "api_cache": os.path.abspath(cache_path),
        "review_file": os.path.abspath(review_path),
        "summary": {
            "affected_brs": len(works),
            "missing_ars": len(
                {uri for missing in missing_by_work.values() for uri in missing}
            ),
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
        },
        "repairs": repairs,
    }
    _write_json(report_path, report)
    write_review_file(review_path, repairs)
    return report


def _plan_repairs(plan: dict[str, object]) -> list[dict[str, object]]:
    raw_repairs = plan["repairs"]
    if not isinstance(raw_repairs, list) or not all(
        isinstance(repair, dict) for repair in raw_repairs
    ):
        raise ValueError("Correction plan repairs must be a list of objects")
    repairs = cast(list[dict[str, object]], raw_repairs)
    for repair in repairs:
        _verify_repair_id(repair)
    return repairs


def _plan_agent_identifiers(agent: dict[str, object]) -> list[tuple[str, str]]:
    raw_identifiers = agent["identifiers"]
    if not isinstance(raw_identifiers, list):
        raise ValueError("Planned agent identifiers must be a list")
    identifiers = []
    for raw_identifier in raw_identifiers:
        if not isinstance(raw_identifier, dict):
            raise ValueError("Planned agent identifier must be an object")
        identifier = cast(dict[str, object], raw_identifier)
        scheme = identifier["scheme"]
        value = identifier["value"]
        if not isinstance(scheme, str) or not isinstance(value, str):
            raise ValueError("Planned agent identifier fields must be strings")
        key = _normalized_identifier(scheme, value)
        if key[0] not in SUPPORTED_AGENT_IDENTIFIERS:
            raise ValueError(f"Unsupported agent identifier scheme: {key[0]}")
        identifiers.append(key)
    return identifiers


def _repair_targets(repair: dict[str, object]) -> Iterator[dict[str, object]]:
    role_plans = repair["role_plans"]
    if not isinstance(role_plans, list):
        raise ValueError("Repair role_plans must be a list")
    for raw_role_plan in role_plans:
        if not isinstance(raw_role_plan, dict):
            raise ValueError("Repair role plan must be an object")
        role_plan = cast(dict[str, object], raw_role_plan)
        raw_targets = role_plan["targets"]
        if not isinstance(raw_targets, list):
            raise ValueError("Repair targets must be a list")
        for raw_target in raw_targets:
            if not isinstance(raw_target, dict):
                raise ValueError("Repair target must be an object")
            yield cast(dict[str, object], raw_target)


def _repair_identifier_keys(
    repairs: list[dict[str, object]],
) -> set[tuple[str, str]]:
    return {
        key
        for repair in repairs
        for target in _repair_targets(repair)
        for key in _plan_agent_identifiers(cast(dict[str, object], target["agent"]))
    }


def _capture_local_state(
    repair: dict[str, object], config: AuditConfig, workers: int
) -> dict[str, object]:
    br_uri = repair["br"]
    if not isinstance(br_uri, str):
        raise ValueError("Repair BR must be a string")
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
    missing_by_work = {br_uri: missing}
    roles, agents, _, raw_entities, provenance = _local_context(
        {br_uri: work}, role_entities, missing_by_work, locator, workers
    )
    return _local_state(work, roles, agents, raw_entities, missing, provenance)


def _validate_identity_index(
    repair: dict[str, object],
    identity_index: dict[tuple[str, str], IdentityEntry],
) -> None:
    for target in _repair_targets(repair):
        agent = cast(dict[str, object], target["agent"])
        keys = _plan_agent_identifiers(agent)
        ra_uris = {ra_uri for key in keys for ra_uri in identity_index[key].ra_uris}
        if len(ra_uris) > 1:
            raise RuntimeError(
                f"Identifier now resolves to multiple RAs in {repair['br']}: "
                f"{sorted(ra_uris)}"
            )
        for key in keys:
            entry = identity_index[key]
            if not entry.ra_uris and len(entry.identifier_uris) > 1:
                raise RuntimeError(
                    f"Identifier now has multiple orphan entities in {repair['br']}: "
                    f"{key}"
                )
        resolution = target["resolution"]
        if resolution == "identifier" and ra_uris != {target["ra"]}:
            raise RuntimeError(
                f"Stale plan: identifier resolution changed for {repair['br']}"
            )


def _source_url(repair: dict[str, object], source: str) -> str | None:
    identifiers = cast(dict[str, str], repair["work_identifiers"])
    doi = identifiers["doi"] if "doi" in identifiers else ""
    openalex_id = identifiers["openalex"] if "openalex" in identifiers else ""
    if source == "crossref" and doi:
        return f"https://api.crossref.org/works/{doi}"
    if source == "datacite" and doi:
        return f"https://api.datacite.org/dois/{doi}"
    if source == "openalex":
        if openalex_id:
            return f"https://api.openalex.org/works/{openalex_id.rsplit('/', 1)[-1]}"
        if doi:
            return f"https://api.openalex.org/works?filter=doi:{doi}"
    return None


def _import_files(
    editor: MetaEditor, g_set: GraphSet, uris: set[str], locator: EntityFileLocator
) -> None:
    paths = {locator.path(uri) for uri in uris}
    for path in sorted(paths):
        if not os.path.exists(path):
            continue
        graph = editor.reader.load(path)
        if graph is not None:
            editor.reader.import_entities_from_graph(
                g_set, graph, editor.resp_agent, enable_validation=False
            )


def _bibliographic_resource(g_set: GraphSet, uri: str) -> BibliographicResource:
    entity = g_set.get_entity(uri)
    if not isinstance(entity, BibliographicResource):
        raise ValueError(f"Bibliographic resource not imported: {uri}")
    return entity


def _entity_uri(entity: AgentRole | ResponsibleAgent | Identifier) -> str:
    return str(entity.res)


def _create_identifier_value(identifier: Identifier, scheme: str, value: str) -> None:
    if scheme == "orcid":
        identifier.create_orcid(value)
    elif scheme == "crossref":
        identifier.create_crossref(value)
    elif scheme == "ror":
        identifier.g.add(
            (
                identifier.res,
                GraphEntity.iri_uses_identifier_scheme,
                RDFTerm("uri", f"{DATACITE_PREFIX}ror"),
            )
        )
        identifier.g.add(
            (
                identifier.res,
                GraphEntity.iri_has_literal_value,
                RDFTerm("literal", value, XSD_STRING),
            )
        )
    else:
        raise ValueError(f"Unsupported agent identifier scheme: {scheme}")


def _set_agent_name(agent: ResponsibleAgent, metadata: dict[str, object]) -> None:
    family = metadata["family"]
    given = metadata["given"]
    name = metadata["name"]
    if not all(isinstance(value, str) for value in (family, given, name)):
        raise ValueError("Planned agent names must be strings")
    if family:
        agent.has_family_name(cast(str, family))
    if given:
        agent.has_given_name(cast(str, given))
    if name and not family and not given:
        agent.has_name(cast(str, name))


def _resolve_target_agent(
    editor: MetaEditor,
    g_set: GraphSet,
    repair: dict[str, object],
    target: dict[str, object],
    identity_index: dict[tuple[str, str], IdentityEntry],
) -> ResponsibleAgent:
    resolution = target["resolution"]
    planned_ra = target["ra"]
    if not isinstance(resolution, str) or not isinstance(planned_ra, str):
        raise ValueError("Invalid planned RA resolution")
    if resolution in {"identifier", "name"}:
        return _responsible_agent(g_set, planned_ra)

    metadata = cast(dict[str, object], target["agent"])
    keys = _plan_agent_identifiers(metadata)
    existing_ras = {ra_uri for key in keys for ra_uri in identity_index[key].ra_uris}
    if existing_ras:
        return _responsible_agent(g_set, next(iter(existing_ras)))

    source = target["source"]
    if not isinstance(source, str):
        raise ValueError("Planned provider source must be a string")
    source_url = _source_url(repair, source)
    agent = g_set.add_ra(editor.resp_agent, source=source_url)
    _set_agent_name(agent, metadata)
    for scheme, value in keys:
        entry = identity_index[(scheme, value)]
        if entry.identifier_uris:
            identifier = _identifier(g_set, entry.identifier_uris[0])
        else:
            identifier = g_set.add_id(editor.resp_agent, source=source_url)
            _create_identifier_value(identifier, scheme, value)
        agent.has_identifier(identifier)
        identity_index[(scheme, value)] = IdentityEntry(
            (_entity_uri(identifier),), (_entity_uri(agent),)
        )
    return agent


def _create_role(
    editor: MetaEditor,
    g_set: GraphSet,
    repair: dict[str, object],
    role_name: str,
    source: str,
) -> AgentRole:
    role = g_set.add_ar(editor.resp_agent, source=_source_url(repair, source))
    if role_name == "author":
        role.create_author()
    elif role_name == "editor":
        role.create_editor()
    elif role_name == "publisher":
        role.create_publisher()
    else:
        raise ValueError(f"Unsupported role: {role_name}")
    return role


def _apply_repair(
    editor: MetaEditor,
    repair: dict[str, object],
    identity_index: dict[tuple[str, str], IdentityEntry],
    locator: EntityFileLocator,
) -> None:
    br_uri = cast(str, repair["br"])
    local_state = cast(dict[str, object], repair["local_state"])
    local_entities = cast(dict[str, object], local_state["entities"])
    import_uris = set(local_entities)
    for target in _repair_targets(repair):
        for key in _plan_agent_identifiers(cast(dict[str, object], target["agent"])):
            import_uris.update(identity_index[key].identifier_uris)
            import_uris.update(identity_index[key].ra_uris)
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=editor.supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    _import_files(editor, g_set, import_uris, locator)
    br = _bibliographic_resource(g_set, br_uri)
    provenance = cast(dict[str, str], repair["missing_ar_provenance"])
    for missing_uri in cast(list[str], repair["missing_ars"]):
        br.g.remove(
            (
                br.res,
                GraphEntity.iri_is_document_context_for,
                RDFTerm("uri", missing_uri),
            )
        )
        if provenance[missing_uri] == "active":
            missing_role = g_set.add_ar(editor.resp_agent, res=missing_uri)
            missing_role.mark_as_to_be_deleted()

    existing_roles = {
        uri: _agent_role(g_set, uri)
        for uri in local_entities
        if "/ar/" in uri and g_set.get_entity(uri) is not None
    }
    for role in existing_roles.values():
        role.remove_next()

    role_plans = cast(list[dict[str, object]], repair["role_plans"])
    for role_plan in role_plans:
        role_name = cast(str, role_plan["role"])
        source = cast(str, role_plan["source"])
        desired_roles = []
        for target in cast(list[dict[str, object]], role_plan["targets"]):
            agent = _resolve_target_agent(editor, g_set, repair, target, identity_index)
            ar_uri = cast(str, target["ar"])
            if ar_uri:
                role = _agent_role(g_set, ar_uri)
                if cast(str, target["old_ra"]) != _entity_uri(agent):
                    role.remove_is_held_by()
                    role.is_held_by(agent)
            else:
                role = _create_role(editor, g_set, repair, role_name, source)
                role.is_held_by(agent)
                br.has_contributor(role)
            desired_roles.append(role)
        for left, right in zip(desired_roles, desired_roles[1:]):
            left.has_next(right)
        for ar_uri in cast(list[str], role_plan["delete_ars"]):
            role = _agent_role(g_set, ar_uri)
            br.remove_contributor(role)
            role.mark_as_to_be_deleted()
    editor.save(g_set, editor.supplier_prefix)


def _write_reindex_sentinel(path: str, plan_path: str) -> None:
    with open(path, "w", encoding="utf-8") as stream:
        stream.write(
            f"{plan_path} changed local RDF files.\nRe-index the triplestore from "
            "the RDF files, then delete this file before another correction or "
            "merge run. Run duplicate detection again before fix_duplicate_ras.\n"
        )


def execute_plan(
    config_path: str,
    plan_path: str,
    review_path: str | None,
    resp_agent: str,
    progress_path: str,
    execution_report_path: str,
    workers: int,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    plan_path = os.path.abspath(plan_path)
    plan = _read_json_object(plan_path)
    if plan["schema_version"] != PLAN_SCHEMA_VERSION:
        raise ValueError(f"Unsupported plan schema: {plan['schema_version']}")
    if plan["complete"] is not True:
        raise ValueError("The correction plan is incomplete and cannot be executed")
    if plan["config_sha256"] != _sha256(config_path):
        raise ValueError("The meta configuration changed after plan generation")
    repairs = _plan_repairs(plan)
    selected_review_path = os.path.abspath(
        review_path or cast(str, plan["review_file"])
    )
    approved = read_review_decisions(selected_review_path, repairs)
    sentinel_path = os.path.join(os.path.dirname(plan_path), REINDEX_SENTINEL_FILENAME)
    if os.path.exists(sentinel_path):
        raise RuntimeError(
            f"{sentinel_path} exists. Re-index the triplestore and remove the "
            "sentinel before executing this plan."
        )

    plan_sha256 = _sha256(plan_path)
    review_sha256 = _sha256(selected_review_path)
    completed = _load_progress(progress_path, plan_sha256, review_sha256)
    approved_ids = {cast(str, repair["group_id"]) for repair in approved}
    unknown_completed = completed - approved_ids
    if unknown_completed:
        raise ValueError(
            f"Progress file contains unknown groups: {sorted(unknown_completed)}"
        )
    config = load_audit_config(config_path)
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    identity_index = scan_identity_index(
        config, _repair_identifier_keys(approved), workers
    )
    attempted = 0
    if approved:
        editor = MetaEditor(config_path, resp_agent, save_queries=True)
        editor.rdf_files_only = True
        try:
            with create_progress() as progress:
                task = progress.add_task(
                    "Applying approved repairs", total=len(approved)
                )
                for repair in approved:
                    group_id = cast(str, repair["group_id"])
                    if group_id in completed:
                        progress.advance(task)
                        continue
                    if _stop_requested:
                        break
                    current_state = _capture_local_state(repair, config, workers)
                    if current_state != repair["local_state"]:
                        raise RuntimeError(
                            f"Stale plan: local RDF state changed for {repair['br']}"
                        )
                    _validate_identity_index(repair, identity_index)
                    attempted += 1
                    _apply_repair(editor, repair, identity_index, locator)
                    completed.add(group_id)
                    _save_progress(progress_path, plan_sha256, review_sha256, completed)
                    progress.advance(task)
        finally:
            if attempted:
                _write_reindex_sentinel(sentinel_path, plan_path)

    complete = len(completed) == len(approved) and not _stop_requested
    execution_report: dict[str, object] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan": plan_path,
        "plan_sha256": plan_sha256,
        "review_file": selected_review_path,
        "review_sha256": review_sha256,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "complete": complete,
        "approved_groups": len(approved),
        "completed_groups": sorted(completed),
        "reindex_sentinel": sentinel_path if attempted else None,
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
            "Find bibliographic resources that reference missing agent roles and "
            "rebuild only their author, editor, and publisher chains from Crossref, "
            "DataCite, and OpenAlex metadata."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta YAML config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run", action="store_true", help="Generate a plan without changing RDF"
    )
    mode.add_argument("--execute", metavar="PLAN", help="Execute an approved plan")
    parser.add_argument("--report-file", help="Dry-run JSON plan path")
    parser.add_argument(
        "--review-file",
        help="Review CSV path; defaults to the path stored in the plan on execution",
    )
    parser.add_argument("--cache-file", help="SQLite API cache path")
    parser.add_argument("--mailto", help="Contact email sent to metadata APIs")
    parser.add_argument(
        "--openalex-api-key",
        default=(
            os.environ["OPENALEX_API_KEY"] if "OPENALEX_API_KEY" in os.environ else ""
        ),
        help="OpenAlex API key; defaults to OPENALEX_API_KEY",
    )
    parser.add_argument(
        "--refresh-cache", action="store_true", help="Refresh cached API responses"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(os.cpu_count() or 1, 16),
        help="Threads used for local RDF scanning",
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
        if not args.report_file or not args.mailto:
            parser.error("--report-file and --mailto are required with --dry-run")
        review_path = args.review_file or f"{args.report_file}.review.csv"
        cache_path = args.cache_file or f"{args.report_file}.cache.sqlite"
        report = analyze_dangling_ars(
            config_path=args.config,
            report_path=os.path.abspath(args.report_file),
            review_path=os.path.abspath(review_path),
            cache_path=os.path.abspath(cache_path),
            mailto=args.mailto,
            workers=args.workers,
            refresh_cache=args.refresh_cache,
            openalex_api_key=args.openalex_api_key,
        )
        summary = cast(dict[str, object], report["summary"])
        console.print(
            f"Plan written to [cyan]{os.path.abspath(args.report_file)}[/cyan]. "
            f"Affected BRs: [cyan]{summary['affected_brs']}[/cyan]; missing ARs: "
            f"[cyan]{summary['missing_ars']}[/cyan]."
        )
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
        review_path=args.review_file,
        resp_agent=args.resp_agent,
        progress_path=progress_path,
        execution_report_path=execution_report_path,
        workers=args.workers,
    )
    console.print(
        f"Execution report written to [cyan]{execution_report_path}[/cyan]. "
        f"Completed groups: "
        f"[cyan]{len(cast(list[str], result['completed_groups']))}[/cyan]."
    )


if __name__ == "__main__":
    main()
