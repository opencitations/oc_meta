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
DCTERMS_TITLE = "http://purl.org/dc/terms/title"

SUPPORTED_AGENT_IDENTIFIERS = frozenset({"orcid", "crossref", "ror"})
ROLE_NAMES = ("author", "editor", "publisher")
CONFIRMED_NAME_SCORE = 0.9
REVIEW_FIELDS: list[str] = [
    "group_id",
    "br",
    "doi",
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
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for partial in executor.map(_provenance_status_batch, _batches(tasks, 24)):
            statuses.update(partial)
    return {uri: statuses[uri] if uri in statuses else "no_snapshot" for uri in uris}


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
) -> list[dict[str, object]]:
    anomalies: list[dict[str, object]] = []
    existing = {uri for uri in work.role_uris if uri in roles}
    for role_uri in sorted(existing):
        role = roles[role_uri]
        if len(contexts[role_uri]) != 1:
            anomalies.append(
                {
                    "type": "multiple_contexts",
                    "ar": role_uri,
                    "contexts": list(contexts[role_uri]),
                }
            )
        if len(role.roles) != 1 or role.roles[0] not in ROLE_MAP:
            anomalies.append(
                {"type": "invalid_role", "ar": role_uri, "roles": list(role.roles)}
            )
        if len(role.holders) != 1:
            anomalies.append(
                {
                    "type": "invalid_holder",
                    "ar": role_uri,
                    "holders": list(role.holders),
                }
            )
        if len(role.next_uris) > 1:
            anomalies.append(
                {"type": "fork", "ar": role_uri, "next": list(role.next_uris)}
            )
        if role.uri in role.next_uris:
            anomalies.append({"type": "self_loop", "ar": role_uri})
        for next_uri in role.next_uris:
            if next_uri not in existing:
                if next_uri not in work.role_uris:
                    anomalies.append(
                        {
                            "type": "cross_context_link",
                            "ar": role_uri,
                            "next": next_uri,
                        }
                    )
                continue
            next_role = roles[next_uri]
            if role.role != next_role.role:
                anomalies.append(
                    {"type": "cross_role_link", "ar": role_uri, "next": next_uri}
                )
    predecessor_counts = Counter(
        next_uri
        for role_uri in existing
        for next_uri in set(roles[role_uri].next_uris)
        if next_uri in existing
    )
    anomalies.extend(
        {
            "type": "multiple_predecessors",
            "ar": role_uri,
            "predecessor_count": count,
        }
        for role_uri, count in sorted(predecessor_counts.items())
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
                anomalies.append({"type": "cycle", "role": role_name})
                break
    return anomalies


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
) -> tuple[dict[str, list[AgentMetadata]], dict[str, str | None]]:
    provider_order = {"crossref": 0, "datacite": 1, "openalex": 2}
    ordered_works = sorted(
        enumerate(works),
        key=lambda item: (provider_order[item[1]["source"]], item[0]),
    )
    targets = {}
    sources: dict[str, str | None] = {}
    for role_name in ROLE_NAMES:
        selected: list[AgentMetadata] = []
        source = None
        for _, work in ordered_works:
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
) -> tuple[
    str | None,
    list[dict[str, object]],
    tuple[str, ...],
    dict[str, object] | None,
]:
    identifiers = _supported_agent_identifiers(agent)
    evidence = [
        {
            "scheme": scheme,
            "value": value,
            "identifier_uris": list(identity_index[(scheme, value)].identifier_uris),
            "ra_uris": list(identity_index[(scheme, value)].ra_uris),
        }
        for scheme, value in identifiers
    ]
    ra_uris = {
        ra_uri
        for identifier in identifiers
        for ra_uri in identity_index[identifier].ra_uris
    }
    if len(ra_uris) > 1:
        return None, evidence, tuple(sorted(ra_uris)), None
    if ra_uris:
        return next(iter(ra_uris)), evidence, (), None
    for scheme, value in identifiers:
        entry = identity_index[(scheme, value)]
        if len(entry.identifier_uris) > 1:
            return (
                None,
                evidence,
                (),
                {
                    "type": "ambiguous_identifier",
                    "role": agent["role"],
                    "position": agent["position"],
                    "identifier_resolutions": evidence,
                },
            )
    return None, evidence, (), None


def _agent_record_plan(agent: AgentRecord) -> dict[str, object]:
    return {
        "family": agent.name.family,
        "given": agent.name.given,
        "name": agent.name.name,
        "identifiers": [
            {"scheme": scheme, "value": value}
            for scheme, value in (
                _normalized_identifier(identifier.scheme, identifier.value)
                for identifier in agent.identifiers
            )
            if scheme in SUPPORTED_AGENT_IDENTIFIERS
        ],
    }


def _preserve_role(
    role_name: str,
    current_roles: list[RoleRecord],
    agents: dict[str, AgentRecord],
) -> dict[str, object]:
    ordered = _ordered_roles(current_roles)
    return {
        "role": role_name,
        "source": None,
        "targets": [
            {
                "position": position,
                "agent": _agent_record_plan(agents[role.holders[0]]),
                "source": None,
                "ar": role.uri,
                "old_ra": role.holders[0],
                "ra": role.holders[0],
                "ra_action": "reuse",
                "resolution": "local",
                "identifier_resolutions": [],
                "old_next": role.next_uris[0] if role.next_uris else None,
            }
            for position, role in enumerate(ordered)
        ],
        "delete_ars": [],
    }


def _reconcile_role(
    role_name: str,
    current_roles: list[RoleRecord],
    external_agents: list[AgentMetadata],
    agents: dict[str, AgentRecord],
    identity_index: dict[tuple[str, str], IdentityEntry],
    source: str,
) -> tuple[dict[str, object], dict[str, object] | None]:
    ordered = _ordered_roles(current_roles)
    available = set(range(len(ordered)))
    targets: list[dict[str, object]] = []
    ambiguous_candidates_by_position: dict[int, tuple[str, ...]] = {}
    for position, external in enumerate(external_agents):
        exact_ra, identifier_resolutions, ambiguous_candidates, blocker = (
            _identity_resolution(external, identity_index)
        )
        if blocker is not None:
            return {}, blocker
        if ambiguous_candidates:
            ambiguous_candidates_by_position[position] = ambiguous_candidates
        resolution = "identifier" if exact_ra is not None else "new"
        selected_index: int | None = None
        if exact_ra is not None:
            matching_roles = [
                index
                for index in sorted(available)
                if ordered[index].holders[0] == exact_ra
            ]
            if matching_roles:
                selected_index = matching_roles[0]
        elif not ambiguous_candidates:
            scored = sorted(
                (
                    name_score(
                        agents[ordered[index].holders[0]].name,
                        _agent_name(external),
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
                    resolution = "name"
        selected_role = ordered[selected_index] if selected_index is not None else None
        if selected_index is not None:
            available.remove(selected_index)
        targets.append(
            {
                "position": position,
                "agent": _agent_plan(external),
                "source": source,
                "ar": selected_role.uri if selected_role is not None else None,
                "old_ra": (
                    selected_role.holders[0] if selected_role is not None else None
                ),
                "ra": exact_ra,
                "ra_action": "reuse" if exact_ra is not None else "create",
                "resolution": resolution,
                "identifier_resolutions": identifier_resolutions,
                "old_next": (
                    selected_role.next_uris[0]
                    if selected_role is not None and selected_role.next_uris
                    else None
                ),
            }
        )

    unassigned_targets = [target for target in targets if target["ar"] is None]
    remaining_roles = [ordered[index] for index in sorted(available)]
    for target, role in zip(unassigned_targets, remaining_roles):
        target["ar"] = role.uri
        target["old_ra"] = role.holders[0]
        target["old_next"] = role.next_uris[0] if role.next_uris else None
        available.remove(ordered.index(role))

    for target in targets:
        if target["resolution"] != "new" or target["ar"] is None:
            continue
        position = cast(int, target["position"])
        external = external_agents[position]
        holder = cast(str, target["old_ra"])
        local = agents[holder]
        compatible = name_score(
            local.name, _agent_name(external)
        ) >= CONFIRMED_NAME_SCORE and not _has_conflicting_identifier(local, external)
        candidates = (
            ambiguous_candidates_by_position[position]
            if position in ambiguous_candidates_by_position
            else ()
        )
        if candidates:
            if compatible and holder in candidates:
                target["ra"] = holder
                target["ra_action"] = "reuse"
                target["resolution"] = "ambiguous_identifier_local"
                continue
            return (
                {},
                {
                    "type": "ambiguous_identifier",
                    "role": role_name,
                    "position": position,
                    "aligned_ar": target["ar"],
                    "aligned_holder": holder,
                    "candidate_ras": list(candidates),
                    "identifier_resolutions": target["identifier_resolutions"],
                },
            )
        if compatible:
            target["ra"] = holder
            target["ra_action"] = "reuse"
            target["resolution"] = "position_name"

    for position, candidates in ambiguous_candidates_by_position.items():
        target = targets[position]
        if target["resolution"] == "ambiguous_identifier_local":
            continue
        return (
            {},
            {
                "type": "ambiguous_identifier",
                "role": role_name,
                "position": position,
                "aligned_ar": target["ar"],
                "aligned_holder": target["old_ra"],
                "candidate_ras": list(candidates),
                "identifier_resolutions": target["identifier_resolutions"],
            },
        )

    deleted = [ordered[index].uri for index in sorted(available)]
    return {
        "role": role_name,
        "source": source,
        "targets": targets,
        "delete_ars": deleted,
    }, None


def _preconditions(
    work: WorkRecord,
    roles: dict[str, RoleRecord],
    agents: dict[str, AgentRecord],
    raw_entities: dict[str, dict[str, object]],
    missing: tuple[str, ...],
    provenance: dict[str, str],
    contexts: dict[str, tuple[str, ...]],
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
        "dangling_ar_references": list(missing),
        "provenance": {uri: provenance[uri] for uri in missing},
        "contexts": {uri: list(contexts[uri]) for uri in sorted(work.role_uris)},
    }


def _dangling_reference_review(
    missing: tuple[str, ...], provenance: dict[str, str]
) -> list[dict[str, object]]:
    references = []
    for uri in missing:
        actions = ["remove_from_br"]
        if provenance[uri] == "latest_snapshot_active":
            actions.append("invalidate_provenance")
        references.append(
            {
                "ar": uri,
                "provenance": provenance[uri],
                "actions": actions,
            }
        )
    return references


def _next_target(
    target: dict[str, object] | None, role_name: str
) -> dict[str, object] | None:
    if target is None:
        return None
    if target["ar"] is not None:
        return {"uri": target["ar"]}
    return {
        "uri": None,
        "role": role_name,
        "position": target["position"],
        "minted_at_execution": True,
    }


def _review_changes(
    dangling_references: list[dict[str, object]],
    role_plans: list[dict[str, object]],
) -> list[dict[str, object]]:
    changes: list[dict[str, object]] = [
        {
            "action": "remove_dangling_reference",
            "ar": reference["ar"],
        }
        for reference in dangling_references
    ]
    for role_plan in role_plans:
        role_name = cast(str, role_plan["role"])
        targets = cast(list[dict[str, object]], role_plan["targets"])
        for target in targets:
            if target["ra_action"] == "create":
                changes.append(
                    {
                        "action": "create_responsible_agent",
                        "role": role_name,
                        "position": target["position"],
                        "agent": target["agent"],
                        "planned_uri": None,
                        "minted_at_execution": True,
                    }
                )
            if target["ar"] is None:
                changes.append(
                    {
                        "action": "create_agent_role",
                        "role": role_name,
                        "position": target["position"],
                        "planned_uri": None,
                        "minted_at_execution": True,
                    }
                )
            elif target["ra_action"] == "create" or target["old_ra"] != target["ra"]:
                changes.append(
                    {
                        "action": "reassign_agent_role",
                        "ar": target["ar"],
                        "old_ra": target["old_ra"],
                        "new_ra": target["ra"],
                        "role": role_name,
                        "position": target["position"],
                    }
                )
        changes.extend(
            {"action": "delete_agent_role", "ar": ar_uri, "role": role_name}
            for ar_uri in cast(list[str], role_plan["delete_ars"])
        )
        for position, target in enumerate(targets):
            if target["ar"] is None:
                continue
            next_target = targets[position + 1] if position + 1 < len(targets) else None
            desired = _next_target(next_target, role_name)
            desired_uri = desired["uri"] if desired is not None else None
            if target["old_next"] == desired_uri and not (
                desired is not None and desired_uri is None
            ):
                continue
            change: dict[str, object] = {
                "action": "update_next_link",
                "ar": target["ar"],
                "old_next": target["old_next"],
                "new_next": desired_uri,
            }
            if desired is not None and desired_uri is None:
                change["new_target"] = desired
            changes.append(change)
    return changes


def _finalize_repair(repair: dict[str, object]) -> dict[str, object]:
    repair["group_id"] = _object_sha256(repair)[:20]
    return repair


def _build_repair(
    draft: dict[str, object],
    roles: dict[str, RoleRecord],
    agents: dict[str, AgentRecord],
    identity_index: dict[tuple[str, str], IdentityEntry],
) -> dict[str, object]:
    review = cast(dict[str, object], draft["review"])
    execution = cast(dict[str, object], draft["execution"])
    problem = cast(dict[str, object], review["problem"])
    dangling_references = cast(
        list[dict[str, object]], problem["dangling_ar_references"]
    )
    status = cast(str, review["status"])
    if status != "ready":
        execution["role_plans"] = []
        review["changes"] = _review_changes(dangling_references, [])
        draft.pop("_work_record")
        draft.pop("_targets")
        return _finalize_repair(draft)
    work = cast(WorkRecord, draft["_work_record"])
    targets = cast(dict[str, list[AgentMetadata]], draft.pop("_targets"))
    sources = cast(dict[str, str | None], review["selected_provider_by_role"])
    role_plans = []
    for role_name in ROLE_NAMES:
        current = [
            roles[uri]
            for uri in work.role_uris
            if uri in roles and roles[uri].role == role_name
        ]
        source = sources[role_name]
        if source is None:
            role_plan = _preserve_role(role_name, current, agents)
            blocker = None
        else:
            role_plan, blocker = _reconcile_role(
                role_name,
                current,
                targets[role_name],
                agents,
                identity_index,
                source,
            )
        if blocker is not None:
            review["status"] = "blocked"
            cast(list[dict[str, object]], review["blockers"]).append(blocker)
            execution["role_plans"] = []
            review["changes"] = _review_changes(dangling_references, [])
            draft.pop("_work_record")
            return _finalize_repair(draft)
        role_plans.append(role_plan)
    execution["role_plans"] = role_plans
    review["changes"] = _review_changes(dangling_references, role_plans)
    draft.pop("_work_record")
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
            blockers = _structural_anomalies(work, roles, contexts)
            for missing_uri in missing_by_work[br_uri]:
                if len(contexts[missing_uri]) != 1:
                    blockers.append(
                        {
                            "type": "multiple_contexts",
                            "ar": missing_uri,
                            "contexts": list(contexts[missing_uri]),
                        }
                    )
            for role_uri in work.role_uris:
                if role_uri in roles and any(
                    holder not in agents for holder in roles[role_uri].holders
                ):
                    blockers.append({"type": "missing_agent", "ar": role_uri})
            work_identifiers = _work_identifiers(work, identifiers)
            doi = work_identifiers["doi"] if "doi" in work_identifiers else ""
            openalex_id = (
                work_identifiers["openalex"] if "openalex" in work_identifiers else ""
            )
            provider_works = (
                [] if blockers else provider.all_work_sources(doi, openalex_id)
            )
            targets, role_sources = select_provider_targets(provider_works)
            warnings = [
                {
                    "type": "role_not_provided",
                    "role": role_name,
                    "action": "preserve_local_chain",
                }
                for role_name in ROLE_NAMES
                if not blockers and role_sources[role_name] is None
            ]
            status = "blocked" if blockers else "ready"
            if not blockers:
                identity_targets.update(_target_identifier_keys(targets))
            work_entity = raw_entities[br_uri]
            titles = _literals(work_entity, DCTERMS_TITLE)
            title = titles[0] if titles else None
            dangling_references = _dangling_reference_review(
                missing_by_work[br_uri], provenance
            )
            drafts.append(
                {
                    "work": {
                        "br": br_uri,
                        "title": title,
                        "identifiers": work_identifiers,
                    },
                    "review": {
                        "status": status,
                        "problem": {
                            "dangling_ar_references": dangling_references,
                        },
                        "provider_records_found": [
                            work_metadata["source"] for work_metadata in provider_works
                        ],
                        "selected_provider_by_role": role_sources,
                        "changes": [],
                        "warnings": warnings,
                        "blockers": blockers,
                    },
                    "execution": {
                        "preconditions": _preconditions(
                            work,
                            roles,
                            agents,
                            raw_entities,
                            missing_by_work[br_uri],
                            provenance,
                            contexts,
                        ),
                        "role_plans": [],
                    },
                    "_work_record": work,
                    "_targets": targets,
                }
            )
            progress.advance(task)
    identity_index = scan_identity_index(config, identity_targets, workers)
    return [_build_repair(draft, roles, agents, identity_index) for draft in drafts]


def _review_row(repair: dict[str, object]) -> dict[str, str | int]:
    work = cast(dict[str, object], repair["work"])
    identifiers = cast(dict[str, str], work["identifiers"])
    review = cast(dict[str, object], repair["review"])
    return {
        "group_id": cast(str, repair["group_id"]),
        "br": cast(str, work["br"]),
        "doi": identifiers["doi"] if "doi" in identifiers else "",
        "status": cast(str, review["status"]),
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
            repair_review = cast(dict[str, object], repairs_by_id[group_id]["review"])
            if decision == "approve" and repair_review["status"] != "ready":
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
    status_counts = Counter(
        cast(str, cast(dict[str, object], repair["review"])["status"])
        for repair in repairs
    )
    blocker_counts = Counter(
        cast(str, blocker["type"])
        for repair in repairs
        for blocker in cast(
            list[dict[str, object]],
            cast(dict[str, object], repair["review"])["blockers"],
        )
    )
    report: dict[str, object] = {
        "complete": not _stop_requested,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": os.path.abspath(config_path),
        "config_sha256": _sha256(config_path),
        "rdf_dir": config.rdf_dir,
        "api_cache": os.path.abspath(cache_path),
        "review_file": os.path.abspath(review_path),
        "summary": {
            "affected_brs": len(works),
            "dangling_ar_references": len(
                {uri for missing in missing_by_work.values() for uri in missing}
            ),
            "status_counts": dict(sorted(status_counts.items())),
            "blocker_counts": dict(sorted(blocker_counts.items())),
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
    execution = repair["execution"]
    if not isinstance(execution, dict):
        raise ValueError("Repair execution must be an object")
    role_plans = execution["role_plans"]
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
        if target["resolution"] != "local"
        for key in _plan_agent_identifiers(cast(dict[str, object], target["agent"]))
    }


def _current_role_contexts(
    config: AuditConfig, role_uris: tuple[str, ...], workers: int
) -> dict[str, tuple[str, ...]]:
    if not role_uris:
        return {}
    role_set = frozenset(role_uris)
    contexts: dict[str, list[str]] = defaultdict(list)
    br_files = _data_files(os.path.join(config.rdf_dir, "br"), config.zip_output)
    batches = _batches(br_files, 24)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for partial in executor.map(
            lambda paths: _scan_role_context_batch(paths, role_set), batches
        ):
            for role_uri, work_uris in partial.items():
                contexts[role_uri].extend(work_uris)
    return {role_uri: tuple(sorted(contexts[role_uri])) for role_uri in role_uris}


def _capture_preconditions(
    repair: dict[str, object],
    config: AuditConfig,
    workers: int,
    role_contexts: dict[str, tuple[str, ...]] | None = None,
) -> dict[str, object]:
    work_summary = repair["work"]
    if not isinstance(work_summary, dict):
        raise ValueError("Repair work must be an object")
    br_uri = work_summary["br"]
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
    contexts = (
        _current_role_contexts(config, work.role_uris, workers)
        if role_contexts is None
        else {
            uri: role_contexts[uri] if uri in role_contexts else ()
            for uri in work.role_uris
        }
    )
    return _preconditions(
        work, roles, agents, raw_entities, missing, provenance, contexts
    )


def _validate_identity_index(
    repair: dict[str, object],
    identity_index: dict[tuple[str, str], IdentityEntry],
) -> None:
    for target in _repair_targets(repair):
        if target["resolution"] == "local":
            continue
        agent = cast(dict[str, object], target["agent"])
        keys = _plan_agent_identifiers(agent)
        raw_resolutions = target["identifier_resolutions"]
        if not isinstance(raw_resolutions, list):
            raise ValueError("Identifier resolutions must be a list")
        planned_entries = {}
        for raw_resolution in raw_resolutions:
            if not isinstance(raw_resolution, dict):
                raise ValueError("Identifier resolution must be an object")
            resolution = cast(dict[str, object], raw_resolution)
            scheme = resolution["scheme"]
            value = resolution["value"]
            identifier_uris = resolution["identifier_uris"]
            ra_uris = resolution["ra_uris"]
            if (
                not isinstance(scheme, str)
                or not isinstance(value, str)
                or not isinstance(identifier_uris, list)
                or not all(isinstance(uri, str) for uri in identifier_uris)
                or not isinstance(ra_uris, list)
                or not all(isinstance(uri, str) for uri in ra_uris)
            ):
                raise ValueError("Invalid identifier resolution")
            planned_entries[_normalized_identifier(scheme, value)] = IdentityEntry(
                tuple(cast(list[str], identifier_uris)),
                tuple(cast(list[str], ra_uris)),
            )
        current_entries = {key: identity_index[key] for key in keys}
        if current_entries != planned_entries:
            work = cast(dict[str, object], repair["work"])
            raise RuntimeError(
                f"Stale plan: identifier resolution changed for {work['br']}"
            )


def _source_url(repair: dict[str, object], source: str) -> str | None:
    work = cast(dict[str, object], repair["work"])
    identifiers = cast(dict[str, str], work["identifiers"])
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
    ra_action = target["ra_action"]
    planned_ra = target["ra"]
    if not isinstance(resolution, str) or ra_action not in {"reuse", "create"}:
        raise ValueError("Invalid planned RA resolution")
    if ra_action == "reuse":
        if not isinstance(planned_ra, str):
            raise ValueError("Reused RA must have a planned URI")
        return _responsible_agent(g_set, planned_ra)
    if planned_ra is not None:
        raise ValueError("Created RA must not have a planned URI")

    metadata = cast(dict[str, object], target["agent"])
    keys = _plan_agent_identifiers(metadata)

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
    work = cast(dict[str, object], repair["work"])
    br_uri = cast(str, work["br"])
    execution = cast(dict[str, object], repair["execution"])
    preconditions = cast(dict[str, object], execution["preconditions"])
    local_entities = cast(dict[str, object], preconditions["entities"])
    import_uris = set(local_entities)
    for target in _repair_targets(repair):
        if target["resolution"] == "local":
            continue
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
    provenance = cast(dict[str, str], preconditions["provenance"])
    for missing_uri in cast(list[str], preconditions["dangling_ar_references"]):
        br.g.remove(
            (
                br.res,
                GraphEntity.iri_is_document_context_for,
                RDFTerm("uri", missing_uri),
            )
        )
        if provenance[missing_uri] == "latest_snapshot_active":
            missing_role = g_set.add_ar(editor.resp_agent, res=missing_uri)
            missing_role.mark_as_to_be_deleted()

    role_plans = cast(list[dict[str, object]], execution["role_plans"])
    for role_plan in role_plans:
        role_name = cast(str, role_plan["role"])
        source = role_plan["source"]
        targets = cast(list[dict[str, object]], role_plan["targets"])
        desired_roles: list[AgentRole] = []
        for target in targets:
            agent = _resolve_target_agent(editor, g_set, repair, target, identity_index)
            ar_uri = target["ar"]
            if isinstance(ar_uri, str):
                role = _agent_role(g_set, ar_uri)
                if cast(str, target["old_ra"]) != _entity_uri(agent):
                    role.remove_is_held_by()
                    role.is_held_by(agent)
            else:
                if ar_uri is not None or not isinstance(source, str):
                    raise ValueError("Created AR must have a provider source")
                role = _create_role(editor, g_set, repair, role_name, source)
                role.is_held_by(agent)
                br.has_contributor(role)
            desired_roles.append(role)
        for position, (target, role) in enumerate(zip(targets, desired_roles)):
            right = (
                desired_roles[position + 1]
                if position + 1 < len(desired_roles)
                else None
            )
            desired_next = _entity_uri(right) if right is not None else None
            if target["ar"] is None:
                if right is not None:
                    role.has_next(right)
                continue
            if target["old_next"] == desired_next:
                continue
            role.remove_next()
            if right is not None:
                role.has_next(right)
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
        pending = [
            repair
            for repair in approved
            if cast(str, repair["group_id"]) not in completed
        ]
        planned_context_uris = set()
        for repair in pending:
            repair_execution = cast(dict[str, object], repair["execution"])
            repair_preconditions = cast(
                dict[str, object], repair_execution["preconditions"]
            )
            planned_context_uris.update(
                cast(dict[str, list[str]], repair_preconditions["contexts"])
            )
        current_contexts = _current_role_contexts(
            config, tuple(sorted(planned_context_uris)), workers
        )
        for repair in pending:
            _validate_identity_index(repair, identity_index)
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
                    current_state = _capture_preconditions(
                        repair, config, workers, current_contexts
                    )
                    execution = cast(dict[str, object], repair["execution"])
                    if current_state != execution["preconditions"]:
                        work = cast(dict[str, object], repair["work"])
                        raise RuntimeError(
                            f"Stale plan: local RDF state changed for {work['br']}"
                        )
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
            f"Affected BRs: [cyan]{summary['affected_brs']}[/cyan]; dangling AR "
            f"references: [cyan]{summary['dangling_ar_references']}[/cyan]."
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
