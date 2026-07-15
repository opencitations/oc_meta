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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from itertools import combinations
from typing import Protocol, cast

import orjson
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.identifier import Identifier
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.agent_matching import (
    AlignmentResult,
    PersonName,
    align_names,
    name_score,
    normalize_name,
    script_family,
)
from oc_meta.lib.agent_metadata import (
    AgentMetadata,
    AgentMetadataClient,
    ApiCache,
    OrcidProfile,
    WorkMetadata,
    agents_for_role,
    is_valid_orcid,
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
from oc_meta.lib.sparql import execute_sparql
from oc_meta.run.merge.entities import REINDEX_SENTINEL_FILENAME

PROV_GENERATED_AT_TIME = "http://www.w3.org/ns/prov#generatedAtTime"
PROV_WAS_ATTRIBUTED_TO = "http://www.w3.org/ns/prov#wasAttributedTo"
PROV_HAD_PRIMARY_SOURCE = "http://www.w3.org/ns/prov#hadPrimarySource"
DCTERMS_DESCRIPTION = "http://purl.org/dc/terms/description"
HAS_UPDATE_QUERY = "https://w3id.org/oc/ontology/hasUpdateQuery"
CONFIRMED_NAME_SCORE = 0.9
AMBIGUOUS_NAME_SCORE = 0.75
PLAN_SCHEMA_VERSION = 1
CLUSTER_BATCH_SIZE = 5000
REVIEW_FIELDS: tuple[str, ...] = (
    "operation_id",
    "csv_row",
    "br",
    "ar",
    "ra",
    "action",
    "identifier_uri",
    "old_value",
    "new_value",
    "confidence",
    "reason",
    "decision",
)

_stop_requested = False


@dataclass(frozen=True, slots=True)
class Cluster:
    csv_row: int
    survivor: str
    members: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IdentifierInfo:
    uri: str
    scheme: str
    value: str


@dataclass(frozen=True, slots=True)
class AgentInfo:
    uri: str
    name: PersonName
    identifiers: tuple[IdentifierInfo, ...]

    @property
    def orcids(self) -> tuple[IdentifierInfo, ...]:
        return tuple(
            identifier
            for identifier in self.identifiers
            if identifier.scheme == "orcid"
        )


@dataclass(frozen=True, slots=True)
class RoleInfo:
    uri: str
    ra: str
    role: str
    next_uris: tuple[str, ...]
    holder_uris: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class WorkInfo:
    uri: str
    identifiers: tuple[IdentifierInfo, ...]
    role_uris: tuple[str, ...]

    def identifier(self, scheme: str) -> str:
        identifier = self.identifier_info(scheme)
        return identifier.value if identifier is not None else ""

    def identifier_info(self, scheme: str) -> IdentifierInfo | None:
        for identifier in self.identifiers:
            if identifier.scheme == scheme:
                return identifier
        return None


@dataclass(frozen=True, slots=True)
class OrderedChain:
    status: str
    roles: tuple[RoleInfo, ...]


@dataclass(frozen=True, slots=True)
class WorkEvidence:
    br: str
    ar: str
    next_uri: str
    work_identifier_uri: str
    work_identifier_scheme: str
    work_identifier_value: str
    role: str
    source: str
    matched: bool
    name_score: float
    api_orcid: str | None
    api_name: str
    contested_elsewhere: bool


class WorkEvidenceClient(Protocol):
    def work_sources(self, doi: str, openalex_id: str = "") -> list[WorkMetadata]: ...


class OrcidClient(Protocol):
    def orcid(self, orcid: str) -> OrcidProfile | None: ...


def _handle_signal(signum: int, frame: object) -> None:
    del signum, frame
    global _stop_requested
    _stop_requested = True


def _entity_name(entity: dict[str, object]) -> PersonName:
    return PersonName(
        name=_first(_literals(entity, FOAF_NAME)),
        given=_first(_literals(entity, GIVEN_NAME)),
        family=_first(_literals(entity, FAMILY_NAME)),
    )


def iter_cluster_batches(
    path: str, batch_size: int = CLUSTER_BATCH_SIZE
) -> Iterator[list[Cluster]]:
    csv.field_size_limit(1024 * 1024 * 1024)
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["surviving_entity", "merged_entities"]:
            raise ValueError(f"Unexpected duplicate CSV header: {reader.fieldnames}")
        batch = []
        for csv_row, row in enumerate(reader, 2):
            merged = tuple(
                item.strip()
                for item in row["merged_entities"].split(";")
                if item.strip()
            )
            members = (row["surviving_entity"].strip(), *merged)
            if not members[0] or len(members) < 2:
                raise ValueError(f"Invalid duplicate cluster at CSV row {csv_row}")
            if len(set(members)) != len(members):
                raise ValueError(f"Repeated entity at CSV row {csv_row}")
            batch.append(Cluster(csv_row, members[0], members))
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _count_duplicate_clusters(path: str) -> int:
    with open(path, "rb") as stream:
        next(stream, None)
        return sum(1 for _ in stream)


def load_target_entities(
    uris: set[str], cache: EntityFileLocator, workers: int
) -> dict[str, dict[str, object]]:
    result = load_available_entities(uris, cache, workers)
    missing = uris - result.keys()
    if missing:
        examples = sorted(missing)[:10]
        raise ValueError(f"RDF entities not found: {examples} ({len(missing)} total)")
    return result


def _identifier_info(
    uri: str, entities: dict[str, dict[str, object]]
) -> IdentifierInfo | None:
    entity = entities.get(uri)
    if entity is None:
        return None
    scheme_uri = _first(_ids(entity, USES_IDENTIFIER_SCHEME))
    value = _first(_literals(entity, HAS_LITERAL_VALUE))
    if not scheme_uri or not value:
        return None
    scheme = (
        scheme_uri[len(DATACITE_PREFIX) :]
        if scheme_uri.startswith(DATACITE_PREFIX)
        else scheme_uri
    )
    return IdentifierInfo(uri, scheme, value)


def load_agents(
    uris: set[str], cache: EntityFileLocator, workers: int
) -> dict[str, AgentInfo]:
    agent_entities = load_target_entities(uris, cache, workers)
    identifier_uris = {
        identifier
        for entity in agent_entities.values()
        for identifier in _ids(entity, HAS_IDENTIFIER)
    }
    identifier_entities = load_target_entities(identifier_uris, cache, workers)
    agents = {}
    for uri, entity in agent_entities.items():
        identifiers = tuple(
            identifier
            for identifier_uri in _ids(entity, HAS_IDENTIFIER)
            if (identifier := _identifier_info(identifier_uri, identifier_entities))
            is not None
        )
        agents[uri] = AgentInfo(uri, _entity_name(entity), identifiers)
    return agents


def scan_candidate_clusters(
    duplicate_path: str,
    cache: EntityFileLocator,
    workers: int,
    all_api: bool,
) -> tuple[list[Cluster], dict[str, AgentInfo], dict[int, list[str]], int, int]:
    candidates = []
    candidate_agents = {}
    risks_by_row = {}
    cluster_count = 0
    agent_count = 0
    total_clusters = _count_duplicate_clusters(duplicate_path)
    with create_progress() as progress:
        task = progress.add_task("Checking duplicate clusters", total=total_clusters)
        for cluster_batch in iter_cluster_batches(duplicate_path):
            if _stop_requested:
                break
            uris = {member for cluster in cluster_batch for member in cluster.members}
            agents = load_agents(uris, cache, workers)
            for cluster in cluster_batch:
                risks = cluster_risks(cluster, agents)
                cluster_count += 1
                agent_count += len(cluster.members)
                if not risks and not all_api:
                    continue
                candidates.append(cluster)
                risks_by_row[cluster.csv_row] = risks
                candidate_agents.update(
                    (member, agents[member]) for member in cluster.members
                )
            progress.advance(task, len(cluster_batch))
    return (
        candidates,
        candidate_agents,
        risks_by_row,
        cluster_count,
        agent_count,
    )


def _load_provenance_batch(
    paths: list[tuple[str, frozenset[str]]],
) -> dict[str, dict[str, object]]:
    snapshots_by_entity: dict[str, list[dict[str, object]]] = defaultdict(list)
    for path, targets in paths:
        if not os.path.exists(path):
            continue
        for snapshot in _load_entities(path).values():
            specializations = _ids(snapshot, PROV_SPECIALIZATION_OF)
            if not specializations or specializations[0] not in targets:
                continue
            snapshots_by_entity[specializations[0]].append(snapshot)

    result = {}
    for uri, snapshots in snapshots_by_entity.items():
        snapshots.sort(
            key=lambda snapshot: _snapshot_number(cast(str, snapshot["@id"]))
        )
        first = snapshots[0]
        latest = snapshots[-1]
        result[uri] = {
            "snapshot_count": len(snapshots),
            "created_at": _first(_literals(first, PROV_GENERATED_AT_TIME)),
            "latest_at": _first(_literals(latest, PROV_GENERATED_AT_TIME)),
            "latest_snapshot": cast(str, latest["@id"]),
            "attributed_to": _ids(latest, PROV_WAS_ATTRIBUTED_TO),
            "primary_sources": _ids(latest, PROV_HAD_PRIMARY_SOURCE),
            "description": _first(_literals(latest, DCTERMS_DESCRIPTION)),
            "update_query": _first(_literals(latest, HAS_UPDATE_QUERY)),
        }
    return result


def load_provenance(
    uris: set[str], cache: EntityFileLocator, workers: int
) -> dict[str, dict[str, object]]:
    targets_by_path: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        path = _provenance_path(cache.path(uri), cache.zip_output)
        targets_by_path[path].add(uri)
    tasks = [(path, frozenset(targets)) for path, targets in targets_by_path.items()]
    result = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for partial in executor.map(_load_provenance_batch, _batches(tasks, 24)):
            result.update(partial)
    return result


def _has_conflicting_names(names: list[PersonName]) -> bool:
    return any(
        name_score(left, right) < CONFIRMED_NAME_SCORE
        for left, right in combinations(names, 2)
    )


def cluster_risks(cluster: Cluster, agents: dict[str, AgentInfo]) -> list[str]:
    names = [agents[uri].name for uri in cluster.members]
    orcids = {
        normalize_orcid(identifier.value)
        for uri in cluster.members
        for identifier in agents[uri].orcids
    }
    risks = []
    if _has_conflicting_names(names):
        risks.append("conflicting_names")
    if len(orcids) > 1:
        risks.append("multiple_orcids")
    if any(len(agents[uri].orcids) > 1 for uri in cluster.members):
        risks.append("bridge_agent")
    if len(cluster.members) >= 50:
        risks.append("large_cluster")
    if any(not name.display for name in names):
        risks.append("missing_name")
    return risks


def _scan_roles_batch(
    paths: list[str], target_ras: frozenset[str] | None
) -> dict[str, RoleInfo]:
    result = {}
    for path in paths:
        for uri, entity in _load_entities(path).items():
            ras = _ids(entity, IS_HELD_BY)
            if not ras or target_ras is not None and target_ras.isdisjoint(ras):
                continue
            roles = _ids(entity, WITH_ROLE)
            result[uri] = RoleInfo(
                uri=uri,
                ra=ras[0],
                role=ROLE_MAP.get(_first(roles), "unknown"),
                next_uris=tuple(_ids(entity, HAS_NEXT)),
                holder_uris=tuple(ras),
            )
    return result


def scan_roles(
    files: list[str], target_ras: set[str] | None, workers: int
) -> dict[str, RoleInfo]:
    targets = frozenset(target_ras) if target_ras is not None else None
    result = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for partial in executor.map(
            lambda paths: _scan_roles_batch(paths, targets), _batches(files, 24)
        ):
            result.update(partial)
            if _stop_requested:
                break
    return result


def _scan_works_batch(
    paths: list[str], target_roles: frozenset[str]
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    result = {}
    for path in paths:
        for uri, entity in _load_entities(path).items():
            roles = tuple(_ids(entity, IS_DOCUMENT_CONTEXT_FOR))
            if target_roles.isdisjoint(roles):
                continue
            result[uri] = (roles, tuple(_ids(entity, HAS_IDENTIFIER)))
    return result


def scan_works(
    files: list[str], target_roles: set[str], workers: int
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    targets = frozenset(target_roles)
    result = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for partial in executor.map(
            lambda paths: _scan_works_batch(paths, targets), _batches(files, 24)
        ):
            result.update(partial)
            if _stop_requested:
                break
    return result


def ordered_chain(roles: list[RoleInfo]) -> OrderedChain:
    if not roles:
        return OrderedChain("empty", ())
    by_uri = {role.uri: role for role in roles}
    if any(
        len(role.holder_uris or ((role.ra,) if role.ra else ())) != 1 for role in roles
    ):
        return OrderedChain("multiple_or_missing_holders", tuple(roles))
    if any(len(role.next_uris) > 1 for role in roles):
        return OrderedChain("fork", tuple(roles))
    if any(next_uri not in by_uri for role in roles for next_uri in role.next_uris):
        return OrderedChain("dangling_or_cross_role", tuple(roles))
    targets = {
        next_uri for role in roles for next_uri in role.next_uris if next_uri in by_uri
    }
    starts = [role for role in roles if role.uri not in targets]
    if len(starts) != 1:
        return OrderedChain("cycle_or_multiple_heads", tuple(roles))
    ordered = []
    seen = set()
    current = starts[0]
    while current.uri not in seen:
        seen.add(current.uri)
        ordered.append(current)
        next_uris = [uri for uri in current.next_uris if uri in by_uri]
        if not next_uris:
            break
        current = by_uri[next_uris[0]]
    if len(ordered) != len(roles):
        return OrderedChain("disconnected_or_cycle", tuple(roles))
    return OrderedChain("valid", tuple(ordered))


def _agent_metadata_name(agent: AgentMetadata) -> PersonName:
    return PersonName(name=agent["name"], given=agent["given"], family=agent["family"])


def _alignment_dict(alignment: AlignmentResult) -> dict[int, tuple[int, float]]:
    return {
        pair.local_index: (pair.external_index, pair.score) for pair in alignment.pairs
    }


def _operation_id(action: str, *parts: str) -> str:
    content = "|".join((action, *parts)).encode()
    return hashlib.sha256(content).hexdigest()[:20]


def _operation(
    action: str,
    csv_row: int,
    reason: str,
    confidence: float,
    *,
    br: str = "",
    ar: str = "",
    ra: str = "",
    identifier_uri: str = "",
    old_value: str = "",
    new_value: str = "",
    links: list[dict[str, str]] | None = None,
    evidence: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    link_value = (
        orjson.dumps(links, option=orjson.OPT_SORT_KEYS).decode()
        if links is not None
        else ""
    )
    evidence_value = (
        orjson.dumps(evidence, option=orjson.OPT_SORT_KEYS).decode()
        if evidence is not None
        else ""
    )
    operation_id = _operation_id(
        action,
        br,
        ar,
        ra,
        identifier_uri,
        old_value,
        new_value,
        link_value,
        evidence_value,
    )
    result: dict[str, object] = {
        "operation_id": operation_id,
        "csv_row": csv_row,
        "action": action,
        "br": br,
        "ar": ar,
        "ra": ra,
        "identifier_uri": identifier_uri,
        "old_value": old_value,
        "new_value": new_value,
        "confidence": round(confidence, 3),
        "reason": reason,
        "approved": False,
    }
    if links is not None:
        result["links"] = links
    if evidence is not None:
        result["evidence"] = evidence
    return result


def _computed_operation_id(operation: dict[str, object]) -> str:
    links = operation["links"] if "links" in operation else None
    evidence = operation["evidence"] if "evidence" in operation else None
    link_value = (
        orjson.dumps(links, option=orjson.OPT_SORT_KEYS).decode()
        if links is not None
        else ""
    )
    evidence_value = (
        orjson.dumps(evidence, option=orjson.OPT_SORT_KEYS).decode()
        if evidence is not None
        else ""
    )
    return _operation_id(
        *(cast(str, operation[field]) for field in ("action", "br", "ar", "ra")),
        cast(str, operation["identifier_uri"]),
        cast(str, operation["old_value"]),
        cast(str, operation["new_value"]),
        link_value,
        evidence_value,
    )


def build_context(
    candidate_ras: set[str],
    rdf_dir: str,
    zip_output: bool,
    cache: EntityFileLocator,
    workers: int,
    max_evidence_works: int,
) -> tuple[dict[str, WorkInfo], dict[str, RoleInfo], dict[str, AgentInfo]]:
    ar_files = _data_files(os.path.join(rdf_dir, "ar"), zip_output)
    candidate_roles = scan_roles(ar_files, candidate_ras, workers)
    br_files = _data_files(os.path.join(rdf_dir, "br"), zip_output)
    raw_works = scan_works(br_files, set(candidate_roles), workers)

    contexts: dict[str, list[str]] = defaultdict(list)
    for work_uri, (role_refs, _) in raw_works.items():
        for role_uri in role_refs:
            role = candidate_roles.get(role_uri)
            if role is not None:
                for holder_uri in role.holder_uris or (role.ra,):
                    if holder_uri in candidate_ras:
                        contexts[holder_uri].append(work_uri)
    selected_work_uris = {
        work_uri
        for ra_uri in candidate_ras
        for work_uri in sorted(set(contexts[ra_uri]))[:max_evidence_works]
    }
    raw_works = {
        work_uri: data
        for work_uri, data in raw_works.items()
        if work_uri in selected_work_uris
    }

    role_uris = {
        role_uri for role_refs, _ in raw_works.values() for role_uri in role_refs
    }
    role_entities = load_target_entities(role_uris, cache, workers)
    roles = {}
    for uri, entity in role_entities.items():
        ras = _ids(entity, IS_HELD_BY)
        role_types = _ids(entity, WITH_ROLE)
        roles[uri] = RoleInfo(
            uri=uri,
            ra=_first(ras),
            role=ROLE_MAP.get(_first(role_types), "unknown"),
            next_uris=tuple(_ids(entity, HAS_NEXT)),
            holder_uris=tuple(ras),
        )

    identifier_uris = {
        identifier_uri
        for _, identifiers in raw_works.values()
        for identifier_uri in identifiers
    }
    identifier_entities = load_target_entities(identifier_uris, cache, workers)
    works = {}
    for uri, (role_refs, identifier_refs) in raw_works.items():
        identifiers = tuple(
            identifier
            for identifier_uri in identifier_refs
            if (identifier := _identifier_info(identifier_uri, identifier_entities))
            is not None
        )
        works[uri] = WorkInfo(uri, identifiers, role_refs)

    ra_uris = {role.ra for role in roles.values() if role.ra}
    agents = load_agents(ra_uris, cache, workers)
    return works, roles, agents


def _role_chains(work: WorkInfo, roles: dict[str, RoleInfo]) -> dict[str, OrderedChain]:
    grouped: dict[str, list[RoleInfo]] = defaultdict(list)
    for role_uri in work.role_uris:
        role = roles.get(role_uri)
        if role is not None:
            grouped[role.role].append(role)
    return {role: ordered_chain(members) for role, members in grouped.items()}


def _selected_work_uris(
    candidate_ras: set[str],
    works: dict[str, WorkInfo],
    roles: dict[str, RoleInfo],
    max_evidence_works: int,
) -> set[str]:
    contexts: dict[str, list[str]] = defaultdict(list)
    for work in works.values():
        for role_uri in work.role_uris:
            role = roles.get(role_uri)
            if role is not None:
                for holder_uri in role.holder_uris or (role.ra,):
                    if holder_uri in candidate_ras:
                        contexts[holder_uri].append(work.uri)
    selected = set()
    for ra in candidate_ras:
        selected.update(sorted(set(contexts[ra]))[:max_evidence_works])
    return selected


def _alignment_report(
    chain: OrderedChain,
    external: list[AgentMetadata],
    agents: dict[str, AgentInfo],
) -> tuple[AlignmentResult | None, dict[str, object]]:
    if chain.status != "valid":
        return None, {
            "chain_status": chain.status,
            "ambiguous": True,
            "pairs": [],
            "unmatched_local": [role.uri for role in chain.roles],
            "unmatched_external": list(range(len(external))),
        }
    local_names = [agents[role.ra].name for role in chain.roles]
    alignment = align_names(
        local_names, [_agent_metadata_name(agent) for agent in external]
    )
    return alignment, {
        "chain_status": chain.status,
        "ambiguous": alignment.ambiguous,
        "pairs": [
            {
                "ar": chain.roles[pair.local_index].uri,
                "ra": chain.roles[pair.local_index].ra,
                "external_position": pair.external_index,
                "score": round(pair.score, 3),
            }
            for pair in alignment.pairs
        ],
        "unmatched_local": [
            chain.roles[index].uri for index in alignment.unmatched_local
        ],
        "unmatched_external": list(alignment.unmatched_external),
    }


def _desired_role_order(
    chain: OrderedChain,
    external: list[AgentMetadata],
    agents: dict[str, AgentInfo],
) -> list[RoleInfo] | None:
    if chain.status != "valid" or len(chain.roles) != len(external):
        return None
    available = set(range(len(chain.roles)))
    desired = []
    for external_agent in external:
        scored = sorted(
            (
                (
                    name_score(
                        agents[chain.roles[index].ra].name,
                        _agent_metadata_name(external_agent),
                    ),
                    index,
                )
                for index in available
            ),
            reverse=True,
        )
        if not scored or scored[0][0] < CONFIRMED_NAME_SCORE:
            return None
        if len(scored) > 1 and abs(scored[0][0] - scored[1][0]) < 1e-9:
            return None
        _, index = scored[0]
        desired.append(chain.roles[index])
        available.remove(index)
    return desired


def _chain_links(roles: list[RoleInfo] | tuple[RoleInfo, ...]) -> list[dict[str, str]]:
    return [
        {
            "ar": role.uri,
            "old_next": _first(list(role.next_uris)),
            "new_next": roles[index + 1].uri if index + 1 < len(roles) else "",
        }
        for index, role in enumerate(roles)
    ]


def _role_operations(
    cluster_by_ra: dict[str, Cluster],
    work: WorkInfo,
    chain: OrderedChain,
    external: list[AgentMetadata],
    agents: dict[str, AgentInfo],
    source: str,
) -> list[dict[str, object]]:
    operations = []
    desired = _desired_role_order(chain, external, agents)
    if desired is not None and [role.uri for role in desired] != [
        role.uri for role in chain.roles
    ]:
        csv_rows = {
            cluster_by_ra[role.ra].csv_row
            for role in chain.roles
            if role.ra in cluster_by_ra
        }
        operations.append(
            _operation(
                "reorder_chain",
                min(csv_rows) if csv_rows else 0,
                f"{source} confirms the same contributors in a different order",
                CONFIRMED_NAME_SCORE,
                br=work.uri,
                links=_chain_links(desired),
            )
        )
        return operations

    if chain.status != "valid":
        return operations
    for position, role in enumerate(chain.roles[: len(external)]):
        current_score = name_score(
            agents[role.ra].name, _agent_metadata_name(external[position])
        )
        if current_score >= AMBIGUOUS_NAME_SCORE or role.ra not in cluster_by_ra:
            continue
        cluster = cluster_by_ra[role.ra]
        candidates = sorted(
            (
                (
                    name_score(
                        agents[member].name, _agent_metadata_name(external[position])
                    ),
                    member,
                )
                for member in cluster.members
                if member in agents and member != role.ra
            ),
            reverse=True,
        )
        if not candidates or candidates[0][0] < CONFIRMED_NAME_SCORE:
            continue
        if len(candidates) > 1 and abs(candidates[0][0] - candidates[1][0]) < 1e-9:
            continue
        score, candidate = candidates[0]
        operations.append(
            _operation(
                "reassign_role",
                cluster.csv_row,
                f"{source} contributor at position {position} matches {candidate}",
                score,
                br=work.uri,
                ar=role.uri,
                ra=role.ra,
                old_value=role.ra,
                new_value=candidate,
            )
        )
    return operations


def collect_external_evidence(
    selected_works: set[str],
    works: dict[str, WorkInfo],
    roles: dict[str, RoleInfo],
    agents: dict[str, AgentInfo],
    cluster_by_ra: dict[str, Cluster],
    client: WorkEvidenceClient,
) -> tuple[
    list[dict[str, object]],
    dict[tuple[str, str], list[WorkEvidence]],
    dict[str, list[PersonName]],
    list[dict[str, object]],
]:
    role_assessments = []
    edge_evidence: dict[tuple[str, str], list[WorkEvidence]] = defaultdict(list)
    names_by_orcid: dict[str, list[PersonName]] = defaultdict(list)
    operations_by_id: dict[str, dict[str, object]] = {}

    with create_progress() as progress:
        task = progress.add_task("Querying work metadata", total=len(selected_works))
        for br_uri in sorted(selected_works):
            if _stop_requested:
                break
            work = works[br_uri]
            doi = work.identifier("doi")
            openalex_id = work.identifier("openalex")
            sources = client.work_sources(doi, openalex_id)
            chains = _role_chains(work, roles)
            if not sources:
                role_assessments.extend(
                    {
                        "br": br_uri,
                        "role": role_name,
                        "source": "",
                        "chain_status": chain.status,
                        "ambiguous": True,
                        "reason": "no_external_work_metadata",
                        "pairs": [],
                        "unmatched_local": [role.uri for role in chain.roles],
                        "unmatched_external": [],
                    }
                    for role_name, chain in chains.items()
                )
            for source_work in sources:
                work_identifier_scheme = (
                    "openalex"
                    if source_work["source"] == "openalex" and openalex_id
                    else "doi"
                )
                work_identifier = work.identifier_info(work_identifier_scheme)
                if work_identifier is None:
                    raise ValueError(
                        f"{source_work['source']} evidence for {br_uri} has no local "
                        f"{work_identifier_scheme} identifier"
                    )
                for role_name, chain in chains.items():
                    external = agents_for_role(source_work, role_name)
                    if not external:
                        role_assessments.append(
                            {
                                "br": br_uri,
                                "role": role_name,
                                "source": source_work["source"],
                                "chain_status": chain.status,
                                "ambiguous": True,
                                "reason": "external_role_missing",
                                "pairs": [],
                                "unmatched_local": [role.uri for role in chain.roles],
                                "unmatched_external": [],
                            }
                        )
                        continue
                    alignment, report = _alignment_report(chain, external, agents)
                    report.update(
                        {
                            "br": br_uri,
                            "role": role_name,
                            "source": source_work["source"],
                        }
                    )
                    role_assessments.append(report)
                    for operation in _role_operations(
                        cluster_by_ra,
                        work,
                        chain,
                        external,
                        agents,
                        source_work["source"],
                    ):
                        operations_by_id[cast(str, operation["operation_id"])] = (
                            operation
                        )
                    if alignment is None:
                        continue
                    matched = _alignment_dict(alignment)
                    for local_index, role in enumerate(chain.roles):
                        if role.ra not in cluster_by_ra:
                            continue
                        for identifier in agents[role.ra].orcids:
                            normalized = normalize_orcid(identifier.value)
                            match = matched.get(local_index)
                            if match is None:
                                evidence = WorkEvidence(
                                    br_uri,
                                    role.uri,
                                    _first(list(role.next_uris)),
                                    work_identifier.uri,
                                    work_identifier.scheme,
                                    work_identifier.value,
                                    role_name,
                                    source_work["source"],
                                    False,
                                    0.0,
                                    None,
                                    "",
                                    False,
                                )
                            else:
                                external_index, score = match
                                api_agent = external[external_index]
                                contested_elsewhere = any(
                                    normalize_orcid(agent["orcid"] or "") == normalized
                                    for index, agent in enumerate(external)
                                    if index != external_index
                                )
                                evidence = WorkEvidence(
                                    br_uri,
                                    role.uri,
                                    _first(list(role.next_uris)),
                                    work_identifier.uri,
                                    work_identifier.scheme,
                                    work_identifier.value,
                                    role_name,
                                    source_work["source"],
                                    score >= CONFIRMED_NAME_SCORE
                                    and not alignment.ambiguous,
                                    score,
                                    normalize_orcid(api_agent["orcid"] or "") or None,
                                    _agent_metadata_name(api_agent).display,
                                    contested_elsewhere,
                                )
                            edge_evidence[(role.ra, normalized)].append(evidence)
                    for api_agent in external:
                        api_orcid = normalize_orcid(api_agent["orcid"] or "")
                        if api_orcid:
                            names_by_orcid[api_orcid].append(
                                _agent_metadata_name(api_agent)
                            )
            progress.advance(task)
    return (
        role_assessments,
        edge_evidence,
        names_by_orcid,
        list(operations_by_id.values()),
    )


def _polluted_identifier(names: list[PersonName]) -> bool:
    for left, right in combinations(names, 2):
        if name_score(left, right) < AMBIGUOUS_NAME_SCORE:
            return True
    return False


def classify_identifiers(
    clusters: list[Cluster],
    risks_by_row: dict[int, list[str]],
    agents: dict[str, AgentInfo],
    edge_evidence: dict[tuple[str, str], list[WorkEvidence]],
    names_by_orcid: dict[str, list[PersonName]],
    provenance: dict[str, dict[str, object]],
    client: OrcidClient,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    assessments = []
    operations = []
    profiles: dict[str, OrcidProfile | None] = {}
    candidate_orcids = {
        normalize_orcid(identifier.value)
        for cluster in clusters
        if cluster.csv_row in risks_by_row
        for member in cluster.members
        for identifier in agents[member].orcids
        if is_valid_orcid(identifier.value)
    }
    with create_progress() as progress:
        task = progress.add_task("Querying ORCID profiles", total=len(candidate_orcids))
        for orcid in sorted(candidate_orcids):
            if _stop_requested:
                break
            profiles[orcid] = client.orcid(orcid)
            progress.advance(task)

    for cluster in clusters:
        if cluster.csv_row not in risks_by_row:
            continue
        for member in cluster.members:
            agent = agents[member]
            for identifier in agent.orcids:
                orcid = normalize_orcid(identifier.value)
                evidence_key = (member, orcid)
                evidence = (
                    edge_evidence[evidence_key] if evidence_key in edge_evidence else []
                )
                confirmed = [item for item in evidence if item.matched]
                positive = [item for item in confirmed if item.api_orcid == orcid]
                different = [
                    item
                    for item in confirmed
                    if item.api_orcid and item.api_orcid != orcid
                ]
                elsewhere = [item for item in confirmed if item.contested_elsewhere]
                profile = profiles.get(orcid)
                profile_name = (
                    PersonName(
                        name=profile["name"],
                        given=profile["given"],
                        family=profile["family"],
                    )
                    if profile is not None
                    else PersonName()
                )
                profile_score = name_score(agent.name, profile_name)
                other_scores = sorted(
                    (
                        name_score(agents[other].name, profile_name),
                        other,
                    )
                    for other in cluster.members
                    if other != member and profile_name.display
                )
                best_other_score, best_other = (
                    other_scores[-1] if other_scores else (0.0, "")
                )
                cross_script = bool(profile_name.display) and script_family(
                    agent.name.display
                ) != script_family(profile_name.display)
                polluted = _polluted_identifier(
                    names_by_orcid[orcid] if orcid in names_by_orcid else []
                )
                replacement_counts = Counter(
                    item.api_orcid for item in different if item.api_orcid
                )
                replacement = ""
                for candidate, votes in replacement_counts.most_common():
                    if candidate not in profiles:
                        profiles[candidate] = client.orcid(candidate)
                    candidate_profile = profiles[candidate]
                    if candidate_profile is None:
                        continue
                    candidate_name = PersonName(
                        name=candidate_profile["name"],
                        given=candidate_profile["given"],
                        family=candidate_profile["family"],
                    )
                    if (
                        votes >= 2
                        and name_score(agent.name, candidate_name)
                        >= CONFIRMED_NAME_SCORE
                    ):
                        replacement = candidate
                        break

                status = "manual_review"
                reason = "Insufficient or conflicting work evidence"
                if not is_valid_orcid(orcid) and confirmed:
                    status = "verified_wrong"
                    reason = "ORCID has an invalid format or checksum"
                elif not is_valid_orcid(orcid):
                    reason = (
                        "ORCID has an invalid format or checksum, but the local "
                        "work responsibility is not externally confirmed"
                    )
                elif (
                    confirmed
                    and profile_name.display
                    and profile_score < AMBIGUOUS_NAME_SCORE
                    and best_other_score >= CONFIRMED_NAME_SCORE
                    and not cross_script
                    and (positive or elsewhere or different or polluted)
                ):
                    status = "verified_wrong"
                    reason = f"ORCID profile matches {best_other}, not {member}"
                elif positive and (
                    not profile_name.display
                    or profile_score >= AMBIGUOUS_NAME_SCORE
                    or cross_script
                ):
                    status = "verified_correct"
                    reason = "Work contributor and local agent use the same ORCID"

                assessment = {
                    "csv_row": cluster.csv_row,
                    "ra": member,
                    "identifier_uri": identifier.uri,
                    "orcid": orcid,
                    "status": status,
                    "reason": reason,
                    "profile": profile,
                    "profile_score": round(profile_score, 3),
                    "best_other_ra": best_other,
                    "best_other_score": round(best_other_score, 3),
                    "replacement_orcid": replacement or None,
                    "work_evidence": [asdict(item) for item in evidence],
                    "agent_provenance": provenance.get(member),
                    "identifier_provenance": provenance.get(identifier.uri),
                }
                assessments.append(assessment)
                if status != "verified_wrong":
                    continue
                action = "replace_identifier" if replacement else "detach_identifier"
                evidence_links = [
                    {
                        "br": br,
                        "ar": ar,
                        "ra": member,
                        "next": next_uri,
                        "work_identifier_uri": work_identifier_uri,
                        "work_identifier_scheme": work_identifier_scheme,
                        "work_identifier_value": work_identifier_value,
                    }
                    for (
                        br,
                        ar,
                        next_uri,
                        work_identifier_uri,
                        work_identifier_scheme,
                        work_identifier_value,
                    ) in sorted(
                        {
                            (
                                item.br,
                                item.ar,
                                item.next_uri,
                                item.work_identifier_uri,
                                item.work_identifier_scheme,
                                item.work_identifier_value,
                            )
                            for item in confirmed
                        }
                    )
                ]
                operations.append(
                    _operation(
                        action,
                        cluster.csv_row,
                        reason,
                        max(best_other_score, CONFIRMED_NAME_SCORE),
                        ra=member,
                        identifier_uri=identifier.uri,
                        old_value=orcid,
                        new_value=replacement,
                        evidence=evidence_links,
                    )
                )
    return assessments, operations


def _review_chain_values(operation: dict[str, object]) -> tuple[str, str]:
    links = operation.get("links")
    if not isinstance(links, list):
        return cast(str, operation["old_value"]), cast(str, operation["new_value"])
    old_values = []
    new_values = []
    for raw_link in links:
        link = cast(dict[str, str], raw_link)
        old_values.append(f"{link['ar']} -> {link['old_next'] or '[end]'}")
        new_values.append(f"{link['ar']} -> {link['new_next'] or '[end]'}")
    return "; ".join(old_values), "; ".join(new_values)


def write_review_file(path: str, operations: list[dict[str, object]]) -> None:
    _ensure_parent(path)
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=REVIEW_FIELDS)
        writer.writeheader()
        for operation in operations:
            old_value, new_value = _review_chain_values(operation)
            row = {
                field: operation[field]
                for field in REVIEW_FIELDS
                if field not in {"old_value", "new_value", "decision"}
            }
            row["old_value"] = old_value
            row["new_value"] = new_value
            row["decision"] = ""
            writer.writerow(row)


def _agent_report(
    agent: AgentInfo, provenance: dict[str, dict[str, object]]
) -> dict[str, object]:
    return {
        "ra": agent.uri,
        "name": asdict(agent.name),
        "normalized_name": normalize_name(agent.name.display),
        "provenance": provenance.get(agent.uri),
        "identifiers": [
            {
                "uri": identifier.uri,
                "scheme": identifier.scheme,
                "value": identifier.value,
                "provenance": provenance.get(identifier.uri),
            }
            for identifier in agent.identifiers
        ],
    }


def analyze_duplicate_ras(
    config_path: str,
    duplicate_path: str,
    report_path: str,
    review_path: str,
    cache_path: str,
    mailto: str,
    workers: int,
    max_evidence_works: int,
    all_api: bool,
    refresh_cache: bool,
    openalex_api_key: str,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    config = load_audit_config(config_path)
    duplicate_path = os.path.abspath(duplicate_path)
    report_path = os.path.abspath(report_path)
    review_path = os.path.abspath(review_path)
    cache_path = os.path.abspath(cache_path)
    cache = EntityFileLocator(
        config.rdf_dir,
        config.dir_split,
        config.items_per_file,
        config.zip_output,
    )
    (
        clusters,
        agents,
        risks_by_row,
        cluster_count,
        agent_count,
    ) = scan_candidate_clusters(duplicate_path, cache, workers, all_api)

    candidate_ras = set(agents)
    if candidate_ras:
        works, roles, contextual_agents = build_context(
            candidate_ras,
            config.rdf_dir,
            config.zip_output,
            cache,
            workers,
            max_evidence_works,
        )
        agents.update(contextual_agents)
    else:
        works, roles = {}, {}
    cluster_by_ra = {
        member: cluster for cluster in clusters for member in cluster.members
    }
    provenance_uris = set(candidate_ras)
    provenance_uris.update(
        identifier.uri for ra in candidate_ras for identifier in agents[ra].identifiers
    )
    provenance = load_provenance(provenance_uris, cache, workers)
    selected_works = _selected_work_uris(
        candidate_ras, works, roles, max_evidence_works
    )

    _ensure_parent(cache_path)
    api_cache = ApiCache(cache_path)
    client = AgentMetadataClient(
        mailto=mailto,
        cache=api_cache,
        refresh_cache=refresh_cache,
        openalex_api_key=openalex_api_key,
    )
    try:
        (
            role_assessments,
            edge_evidence,
            names_by_orcid,
            role_operations,
        ) = collect_external_evidence(
            selected_works,
            works,
            roles,
            agents,
            cluster_by_ra,
            client,
        )
        identifier_assessments, identifier_operations = classify_identifiers(
            clusters,
            risks_by_row,
            agents,
            edge_evidence,
            names_by_orcid,
            provenance,
            client,
        )
    finally:
        client.close()
        api_cache.close()

    operations_by_id = {
        cast(str, operation["operation_id"]): operation
        for operation in (*role_operations, *identifier_operations)
    }
    operations = sorted(
        operations_by_id.values(),
        key=lambda operation: cast(str, operation["operation_id"]),
    )
    risk_counts = Counter(risk for risks in risks_by_row.values() for risk in risks)
    identifier_status_counts = Counter(
        cast(str, assessment["status"]) for assessment in identifier_assessments
    )
    operation_counts = Counter(
        cast(str, operation["action"]) for operation in operations
    )
    report: dict[str, object] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "complete": not _stop_requested,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": os.path.abspath(config_path),
        "config_sha256": _sha256(config_path),
        "duplicates": duplicate_path,
        "duplicates_sha256": _sha256(duplicate_path),
        "rdf_dir": config.rdf_dir,
        "api_cache": cache_path,
        "audit_options": {
            "all_api": all_api,
            "max_evidence_works": max_evidence_works,
            "refresh_cache": refresh_cache,
        },
        "review_file": review_path,
        "summary": {
            "total_clusters": cluster_count,
            "total_cluster_members": agent_count,
            "candidate_clusters": len(clusters),
            "candidate_agents": len(candidate_ras),
            "locally_consistent_clusters": cluster_count
            - sum(bool(risks) for risks in risks_by_row.values()),
            "selected_works": len(selected_works),
            "risk_counts": dict(sorted(risk_counts.items())),
            "identifier_status_counts": dict(sorted(identifier_status_counts.items())),
            "operation_counts": dict(sorted(operation_counts.items())),
        },
        "clusters": [
            {
                "csv_row": cluster.csv_row,
                "survivor": cluster.survivor,
                "merge_status": "blocked_pending_review",
                "risks": risks_by_row[cluster.csv_row],
                "members": [
                    _agent_report(agents[member], provenance)
                    for member in cluster.members
                ],
            }
            for cluster in clusters
        ],
        "role_assessments": role_assessments,
        "identifier_assessments": identifier_assessments,
        "operations": operations,
    }
    _write_json(report_path, report)
    write_review_file(review_path, operations)
    return report


def read_review_decisions(
    path: str, operations: list[dict[str, object]]
) -> list[dict[str, object]]:
    operations_by_id = {
        cast(str, operation["operation_id"]): operation for operation in operations
    }
    if len(operations_by_id) != len(operations):
        raise ValueError("Correction plan contains repeated operation IDs")
    changed_ids = sorted(
        operation_id
        for operation_id, operation in operations_by_id.items()
        if _computed_operation_id(operation) != operation_id
    )
    if changed_ids:
        raise ValueError(f"Correction plan has modified operations: {changed_ids}")
    decisions = {}
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != list(REVIEW_FIELDS):
            raise ValueError(f"Unexpected review CSV header: {reader.fieldnames}")
        for row in reader:
            operation_id = row["operation_id"]
            if operation_id in decisions:
                raise ValueError(f"Repeated review operation: {operation_id}")
            if operation_id not in operations_by_id:
                raise ValueError(f"Unknown review operation: {operation_id}")
            operation = operations_by_id[operation_id]
            old_value, new_value = _review_chain_values(operation)
            expected = {
                field: str(operation[field])
                for field in REVIEW_FIELDS
                if field not in {"old_value", "new_value", "decision"}
            }
            expected["old_value"] = old_value
            expected["new_value"] = new_value
            changed = [
                field
                for field in REVIEW_FIELDS
                if field != "decision" and row[field] != expected[field]
            ]
            if changed:
                raise ValueError(
                    f"Review row {operation_id} differs from the plan in: {changed}"
                )
            decision = row["decision"].strip().lower()
            if decision not in {"", "approve", "reject"}:
                raise ValueError(
                    f"Invalid decision for {operation_id}: {row['decision']}"
                )
            decisions[operation_id] = decision
    missing = operations_by_id.keys() - decisions.keys()
    if missing:
        raise ValueError(f"Review CSV is missing operations: {sorted(missing)}")
    return [
        operation
        for operation_id, operation in operations_by_id.items()
        if decisions[operation_id] == "approve"
    ]


def _validate_uri(uri: str) -> None:
    if not uri.startswith(("http://", "https://")) or any(
        character in uri for character in "<> \t\r\n"
    ):
        raise ValueError(f"Invalid URI in correction plan: {uri}")


def _sparql_bindings(endpoint: str, query: str) -> list[dict[str, dict[str, str]]]:
    result = execute_sparql(endpoint, query, max_retries=3, backoff_factor=1)
    result_section = cast(dict[str, object], result["results"])
    return cast(list[dict[str, dict[str, str]]], result_section["bindings"])


def _current_objects(endpoint: str, subject: str, predicate: str) -> list[str]:
    _validate_uri(subject)
    _validate_uri(predicate)
    query = f"SELECT ?value WHERE {{ <{subject}> <{predicate}> ?value . }}"
    return sorted(
        binding["value"]["value"] for binding in _sparql_bindings(endpoint, query)
    )


def _find_orcid_identifier(endpoint: str, orcid: str) -> str:
    if not is_valid_orcid(orcid):
        raise ValueError(f"Invalid replacement ORCID: {orcid}")
    literal = orjson.dumps(normalize_orcid(orcid)).decode()
    query = f"""
        SELECT DISTINCT ?id WHERE {{
          ?id <{USES_IDENTIFIER_SCHEME}> <{DATACITE_PREFIX}orcid> ;
              <{HAS_LITERAL_VALUE}> {literal} .
        }}
    """
    identifiers = sorted(
        binding["id"]["value"] for binding in _sparql_bindings(endpoint, query)
    )
    if len(identifiers) > 1:
        raise ValueError(
            f"Replacement ORCID {orcid} has multiple identifier entities: {identifiers}"
        )
    return identifiers[0] if identifiers else ""


def _operation_string(operation: dict[str, object], field: str) -> str:
    value = operation[field]
    if not isinstance(value, str):
        raise ValueError(f"Operation field {field} must be a string")
    return value


def _operation_links(operation: dict[str, object]) -> list[dict[str, str]]:
    value = operation["links"]
    if not isinstance(value, list):
        raise ValueError("Reorder operation links must be a list")
    links = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Reorder operation link must be an object")
        link = cast(dict[str, object], item)
        parsed = {}
        for field in ("ar", "old_next", "new_next"):
            field_value = link[field]
            if not isinstance(field_value, str):
                raise ValueError(f"Reorder link field {field} must be a string")
            parsed[field] = field_value
        links.append(parsed)
    return links


def _operation_evidence(operation: dict[str, object]) -> list[dict[str, str]]:
    value = operation["evidence"] if "evidence" in operation else []
    if not isinstance(value, list):
        raise ValueError("Identifier operation evidence must be a list")
    evidence = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Identifier evidence must be an object")
        raw_evidence = cast(dict[str, object], item)
        parsed = {}
        for field in (
            "br",
            "ar",
            "ra",
            "next",
            "work_identifier_uri",
            "work_identifier_scheme",
            "work_identifier_value",
        ):
            field_value = raw_evidence[field]
            if not isinstance(field_value, str):
                raise ValueError(f"Identifier evidence field {field} must be a string")
            parsed[field] = field_value
        evidence.append(parsed)
    return evidence


def _import_entities(editor: MetaEditor, g_set: GraphSet, uris: set[str]) -> None:
    editor.reader.import_entities_from_triplestore(
        g_set=g_set,
        ts_url=editor.endpoint,
        entities=sorted(uris),
        resp_agent=editor.resp_agent,
        enable_validation=False,
    )


def _preflight_operations(
    editor: MetaEditor, operations: list[dict[str, object]]
) -> tuple[set[str], dict[str, str]]:
    uris = set()
    replacements = {}
    for operation in operations:
        action = _operation_string(operation, "action")
        if action in {"detach_identifier", "replace_identifier"}:
            ra_uri = _operation_string(operation, "ra")
            identifier_uri = _operation_string(operation, "identifier_uri")
            old_value = normalize_orcid(_operation_string(operation, "old_value"))
            _validate_uri(ra_uri)
            _validate_uri(identifier_uri)
            if identifier_uri not in _current_objects(
                editor.endpoint, ra_uri, HAS_IDENTIFIER
            ):
                raise RuntimeError(
                    f"Stale plan: {ra_uri} no longer has identifier {identifier_uri}"
                )
            schemes = _current_objects(
                editor.endpoint, identifier_uri, USES_IDENTIFIER_SCHEME
            )
            values = _current_objects(
                editor.endpoint, identifier_uri, HAS_LITERAL_VALUE
            )
            if schemes != [f"{DATACITE_PREFIX}orcid"] or [
                normalize_orcid(value) for value in values
            ] != [old_value]:
                raise RuntimeError(
                    f"Stale plan: identifier {identifier_uri} no longer represents "
                    f"ORCID {old_value}"
                )
            for evidence in _operation_evidence(operation):
                br_uri = evidence["br"]
                ar_uri = evidence["ar"]
                evidence_ra = evidence["ra"]
                work_identifier_uri = evidence["work_identifier_uri"]
                for uri in (
                    br_uri,
                    ar_uri,
                    evidence_ra,
                    work_identifier_uri,
                ):
                    _validate_uri(uri)
                if work_identifier_uri not in _current_objects(
                    editor.endpoint, br_uri, HAS_IDENTIFIER
                ):
                    raise RuntimeError(
                        f"Stale plan: {br_uri} no longer has work identifier "
                        f"{work_identifier_uri}"
                    )
                work_schemes = _current_objects(
                    editor.endpoint, work_identifier_uri, USES_IDENTIFIER_SCHEME
                )
                work_values = _current_objects(
                    editor.endpoint, work_identifier_uri, HAS_LITERAL_VALUE
                )
                if work_schemes != [
                    f"{DATACITE_PREFIX}{evidence['work_identifier_scheme']}"
                ] or work_values != [evidence["work_identifier_value"]]:
                    raise RuntimeError(
                        f"Stale plan: work identifier {work_identifier_uri} changed"
                    )
                if ar_uri not in _current_objects(
                    editor.endpoint, br_uri, IS_DOCUMENT_CONTEXT_FOR
                ):
                    raise RuntimeError(
                        f"Stale plan: {br_uri} no longer contains role {ar_uri}"
                    )
                if _current_objects(editor.endpoint, ar_uri, IS_HELD_BY) != [
                    evidence_ra
                ]:
                    raise RuntimeError(
                        f"Stale plan: {ar_uri} is no longer held by {evidence_ra}"
                    )
                expected_next = [evidence["next"]] if evidence["next"] else []
                if _current_objects(editor.endpoint, ar_uri, HAS_NEXT) != expected_next:
                    raise RuntimeError(
                        f"Stale plan: {ar_uri} hasNext no longer matches the "
                        "confirmed work evidence"
                    )
            uris.update((ra_uri, identifier_uri))
            if action == "replace_identifier":
                replacement = normalize_orcid(_operation_string(operation, "new_value"))
                if replacement not in replacements:
                    replacements[replacement] = _find_orcid_identifier(
                        editor.endpoint, replacement
                    )
                if replacements[replacement]:
                    uris.add(replacements[replacement])
        elif action == "reassign_role":
            br_uri = _operation_string(operation, "br")
            ar_uri = _operation_string(operation, "ar")
            old_ra = _operation_string(operation, "old_value")
            new_ra = _operation_string(operation, "new_value")
            for uri in (br_uri, ar_uri, old_ra, new_ra):
                _validate_uri(uri)
            if ar_uri not in _current_objects(
                editor.endpoint, br_uri, IS_DOCUMENT_CONTEXT_FOR
            ):
                raise RuntimeError(
                    f"Stale plan: {br_uri} no longer contains role {ar_uri}"
                )
            if _current_objects(editor.endpoint, ar_uri, IS_HELD_BY) != [old_ra]:
                raise RuntimeError(
                    f"Stale plan: {ar_uri} is no longer held by {old_ra}"
                )
            uris.update((ar_uri, old_ra, new_ra))
        elif action == "reorder_chain":
            br_uri = _operation_string(operation, "br")
            _validate_uri(br_uri)
            contributor_uris = _current_objects(
                editor.endpoint, br_uri, IS_DOCUMENT_CONTEXT_FOR
            )
            for link in _operation_links(operation):
                ar_uri = link["ar"]
                _validate_uri(ar_uri)
                if ar_uri not in contributor_uris:
                    raise RuntimeError(
                        f"Stale plan: {br_uri} no longer contains role {ar_uri}"
                    )
                current = _current_objects(editor.endpoint, ar_uri, HAS_NEXT)
                expected = [link["old_next"]] if link["old_next"] else []
                if current != expected:
                    raise RuntimeError(
                        f"Stale plan: {ar_uri} hasNext is {current}, expected {expected}"
                    )
                uris.add(ar_uri)
                if link["new_next"]:
                    _validate_uri(link["new_next"])
                    uris.add(link["new_next"])
    return uris, replacements


def _apply_operation_group(
    editor: MetaEditor, operations: list[dict[str, object]]
) -> None:
    uris, replacements = _preflight_operations(editor, operations)

    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=editor.supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    _import_entities(editor, g_set, uris)
    created_replacements: dict[str, Identifier] = {}
    for operation in operations:
        action = _operation_string(operation, "action")
        if action in {"detach_identifier", "replace_identifier"}:
            ra = _responsible_agent(g_set, _operation_string(operation, "ra"))
            identifier_uri = _operation_string(operation, "identifier_uri")
            ra.remove_identifier(_identifier(g_set, identifier_uri))
            if action == "replace_identifier":
                replacement = normalize_orcid(_operation_string(operation, "new_value"))
                replacement_uri = replacements[replacement]
                if replacement_uri:
                    replacement_identifier = _identifier(g_set, replacement_uri)
                elif replacement in created_replacements:
                    replacement_identifier = created_replacements[replacement]
                else:
                    replacement_identifier = g_set.add_id(editor.resp_agent)
                    replacement_identifier.create_orcid(replacement)
                    created_replacements[replacement] = replacement_identifier
                ra.has_identifier(replacement_identifier)
        elif action == "reassign_role":
            role = _agent_role(g_set, _operation_string(operation, "ar"))
            role.remove_is_held_by()
            role.is_held_by(
                _responsible_agent(g_set, _operation_string(operation, "new_value"))
            )
        elif action == "reorder_chain":
            for link in _operation_links(operation):
                role = _agent_role(g_set, link["ar"])
                role.remove_next()
                if link["new_next"]:
                    role.has_next(_agent_role(g_set, link["new_next"]))
    editor.save(g_set, editor.supplier_prefix)


def _validate_approved_operations(operations: list[dict[str, object]]) -> None:
    allowed = {
        "detach_identifier",
        "replace_identifier",
        "reassign_role",
        "reorder_chain",
    }
    identifiers = set()
    assignments = {}
    reorders = {}
    for operation in operations:
        action = _operation_string(operation, "action")
        if action not in allowed:
            raise ValueError(f"Unsupported approved operation: {action}")
        if action in {"detach_identifier", "replace_identifier"}:
            key = (
                _operation_string(operation, "ra"),
                _operation_string(operation, "identifier_uri"),
            )
            if key in identifiers:
                raise ValueError(f"Conflicting identifier operations for {key}")
            identifiers.add(key)
            evidence = _operation_evidence(operation)
            if not evidence:
                raise ValueError(f"Identifier operation has no work evidence: {key}")
            if any(item["ra"] != key[0] for item in evidence):
                raise ValueError(f"Identifier evidence has another RA: {key}")
            if action == "replace_identifier":
                old_value = normalize_orcid(_operation_string(operation, "old_value"))
                new_value = normalize_orcid(_operation_string(operation, "new_value"))
                if old_value == new_value:
                    raise ValueError(f"ORCID replacement does not change {key}")
                if not is_valid_orcid(new_value):
                    raise ValueError(f"Invalid replacement ORCID: {new_value}")
        elif action == "reassign_role":
            ar_uri = _operation_string(operation, "ar")
            new_ra = _operation_string(operation, "new_value")
            if ar_uri in assignments and assignments[ar_uri] != new_ra:
                raise ValueError(f"Conflicting role assignments for {ar_uri}")
            assignments[ar_uri] = new_ra
        else:
            br_uri = _operation_string(operation, "br")
            links = orjson.dumps(_operation_links(operation))
            if br_uri in reorders and reorders[br_uri] != links:
                raise ValueError(f"Conflicting chain orders for {br_uri}")
            reorders[br_uri] = links


def _operation_resources(operation: dict[str, object]) -> set[str]:
    action = _operation_string(operation, "action")
    if action in {"detach_identifier", "replace_identifier"}:
        resources = {
            _operation_string(operation, "ra"),
            _operation_string(operation, "identifier_uri"),
        }
        if action == "replace_identifier":
            resources.add(
                f"orcid:{normalize_orcid(_operation_string(operation, 'new_value'))}"
            )
        for evidence in _operation_evidence(operation):
            resources.update(
                {
                    f"context:{evidence['br']}",
                    evidence["ar"],
                    evidence["ra"],
                    evidence["work_identifier_uri"],
                }
            )
            if evidence["next"]:
                resources.add(evidence["next"])
        return resources
    if action == "reassign_role":
        return {
            f"context:{_operation_string(operation, 'br')}",
            _operation_string(operation, "ar"),
            _operation_string(operation, "old_value"),
            _operation_string(operation, "new_value"),
        }
    resources = {f"context:{_operation_string(operation, 'br')}"}
    for link in _operation_links(operation):
        resources.add(link["ar"])
        if link["old_next"]:
            resources.add(link["old_next"])
        if link["new_next"]:
            resources.add(link["new_next"])
    return resources


def _operation_groups(
    operations: list[dict[str, object]],
) -> list[tuple[str, list[dict[str, object]]]]:
    parents = list(range(len(operations)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    owners = {}
    for index, operation in enumerate(operations):
        for resource in _operation_resources(operation):
            if resource in owners:
                union(index, owners[resource])
            else:
                owners[resource] = index

    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for index, operation in enumerate(operations):
        grouped[find(index)].append(operation)
    result = []
    for group in grouped.values():
        group.sort(key=lambda operation: _operation_string(operation, "operation_id"))
        operation_ids = [
            _operation_string(operation, "operation_id") for operation in group
        ]
        result.append((_operation_id("group", *operation_ids), group))
    return sorted(result, key=lambda item: item[0])


def _write_reindex_sentinel(path: str, plan_path: str) -> None:
    with open(path, "w", encoding="utf-8") as stream:
        stream.write(
            f"{plan_path} changed RDF files on top of the current triplestore "
            "snapshot.\nRe-index the triplestore from the RDF files, then delete "
            "this file before another correction or merge run. Do not reuse the "
            "original duplicate CSV: run duplicate detection again.\n"
        )


def execute_plan(
    config_path: str,
    plan_path: str,
    review_path: str | None,
    resp_agent: str,
    progress_path: str,
    execution_report_path: str,
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
    duplicate_path = cast(str, plan["duplicates"])
    if plan["duplicates_sha256"] != _sha256(duplicate_path):
        raise ValueError("The duplicate CSV changed after plan generation")
    raw_operations = plan["operations"]
    if not isinstance(raw_operations, list) or not all(
        isinstance(operation, dict) for operation in raw_operations
    ):
        raise ValueError("Correction plan operations must be a list of objects")
    operations = cast(list[dict[str, object]], raw_operations)
    selected_review_path = os.path.abspath(
        review_path or cast(str, plan["review_file"])
    )
    approved = read_review_decisions(selected_review_path, operations)
    _validate_approved_operations(approved)

    sentinel_path = os.path.join(os.path.dirname(plan_path), REINDEX_SENTINEL_FILENAME)
    if os.path.exists(sentinel_path):
        raise RuntimeError(
            f"{sentinel_path} exists. Re-index the triplestore and remove the "
            "sentinel before executing this plan."
        )
    plan_sha256 = _sha256(plan_path)
    review_sha256 = _sha256(selected_review_path)
    completed = _load_progress(progress_path, plan_sha256, review_sha256)
    groups = _operation_groups(approved)
    unknown_completed = completed - {group_id for group_id, _ in groups}
    if unknown_completed:
        raise ValueError(
            f"Progress file contains unknown groups: {sorted(unknown_completed)}"
        )
    attempted_groups = 0
    if groups:
        editor = MetaEditor(config_path, resp_agent, save_queries=True)
        try:
            with create_progress() as progress:
                task = progress.add_task(
                    "Applying approved corrections", total=len(groups)
                )
                for group_id, group_operations in groups:
                    if group_id in completed:
                        progress.advance(task)
                        continue
                    if _stop_requested:
                        break
                    attempted_groups += 1
                    _apply_operation_group(editor, group_operations)
                    completed.add(group_id)
                    _save_progress(progress_path, plan_sha256, review_sha256, completed)
                    progress.advance(task)
        finally:
            if attempted_groups:
                _write_reindex_sentinel(sentinel_path, plan_path)

    complete = len(completed) == len(groups) and not _stop_requested
    execution_report: dict[str, object] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan": plan_path,
        "plan_sha256": plan_sha256,
        "review_file": selected_review_path,
        "review_sha256": review_sha256,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "complete": complete,
        "approved_operations": len(approved),
        "total_groups": len(groups),
        "completed_groups": sorted(completed),
        "reindex_sentinel": sentinel_path if attempted_groups else None,
    }
    _write_json(execution_report_path, execution_report)
    if complete and os.path.exists(progress_path):
        os.remove(progress_path)
    return execution_report


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Audit duplicate responsible-agent clusters against local role chains, "
            "Crossref, DataCite, OpenAlex, and ORCID. Apply only operations approved "
            "in the generated review CSV."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta YAML config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run", action="store_true", help="Generate a plan without changing RDF"
    )
    mode.add_argument("--execute", metavar="PLAN", help="Execute an approved plan")
    parser.add_argument(
        "--duplicates", help="Duplicate RA CSV produced by find.duplicates"
    )
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
        "--workers",
        type=int,
        default=min(os.cpu_count() or 1, 16),
        help="Threads used for local RDF scanning",
    )
    parser.add_argument(
        "--max-evidence-works",
        type=int,
        default=5,
        help="Maximum works queried per candidate RA",
    )
    parser.add_argument(
        "--all-api",
        action="store_true",
        help="Query external APIs for every cluster instead of local suspects only",
    )
    parser.add_argument(
        "--refresh-cache", action="store_true", help="Refresh cached API responses"
    )
    parser.add_argument("-r", "--resp-agent", help="Provenance responsible-agent URI")
    parser.add_argument("--progress-file", help="Execution progress JSON path")
    parser.add_argument("--execution-report", help="Execution result JSON path")
    args = parser.parse_args()

    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.max_evidence_works < 1:
        parser.error("--max-evidence-works must be positive")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    if args.dry_run:
        if not args.duplicates or not args.report_file or not args.mailto:
            parser.error(
                "--duplicates, --report-file, and --mailto are required with --dry-run"
            )
        review_path = args.review_file or f"{args.report_file}.review.csv"
        cache_path = args.cache_file or f"{args.report_file}.cache.sqlite"
        report = analyze_duplicate_ras(
            config_path=args.config,
            duplicate_path=args.duplicates,
            report_path=args.report_file,
            review_path=review_path,
            cache_path=cache_path,
            mailto=args.mailto,
            workers=args.workers,
            max_evidence_works=args.max_evidence_works,
            all_api=args.all_api,
            refresh_cache=args.refresh_cache,
            openalex_api_key=args.openalex_api_key,
        )
        summary = cast(dict[str, object], report["summary"])
        console.print(
            f"Plan written to [cyan]{os.path.abspath(args.report_file)}[/cyan]. "
            f"Candidate clusters: [cyan]{summary['candidate_clusters']}[/cyan]; "
            f"proposed operations: [cyan]{sum(cast(dict[str, int], summary['operation_counts']).values())}[/cyan]."
        )
        return

    if not args.resp_agent:
        parser.error("--resp-agent is required with --execute")
    _validate_uri(args.resp_agent)
    plan_path = cast(str, args.execute)
    progress_path = args.progress_file or f"{plan_path}.progress.json"
    execution_report_path = args.execution_report or f"{plan_path}.execution.json"
    result = execute_plan(
        config_path=args.config,
        plan_path=plan_path,
        review_path=args.review_file,
        resp_agent=args.resp_agent,
        progress_path=os.path.abspath(progress_path),
        execution_report_path=os.path.abspath(execution_report_path),
    )
    console.print(
        f"Execution report written to [cyan]{os.path.abspath(execution_report_path)}[/cyan]. "
        f"Completed groups: [cyan]{len(cast(list[str], result['completed_groups']))}[/cyan]."
    )


if __name__ == "__main__":
    main()
