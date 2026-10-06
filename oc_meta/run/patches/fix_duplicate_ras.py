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
from tempfile import TemporaryDirectory
from typing import Protocol, cast

import orjson
from oc_ocdm.graph import GraphSet
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
PLAN_SCHEMA_VERSION = 2
CLUSTER_BATCH_SIZE = 5000
MERGE_FIELDS = ("surviving_entity", "merged_entities")
DEFERRED_FIELDS = (*MERGE_FIELDS, "risks")
REVIEW_FIELDS: tuple[str, ...] = (
    "operation_id",
    "csv_row",
    "ra",
    "action",
    "identifier_uri",
    "old_value",
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
    def crossref(self, doi: str) -> WorkMetadata | None: ...

    def datacite(self, doi: str) -> WorkMetadata | None: ...

    def openalex_work(
        self, doi: str = "", openalex_id: str = ""
    ) -> WorkMetadata | None: ...


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
        if reader.fieldnames not in [list(MERGE_FIELDS), list(DEFERRED_FIELDS)]:
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
                if not risks:
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
    names = list(dict.fromkeys(agents[uri].name for uri in cluster.members))
    identifiers_by_agent = [
        {
            (
                identifier.scheme,
                normalize_orcid(identifier.value)
                if identifier.scheme == "orcid"
                else identifier.value,
            )
            for identifier in agents[uri].identifiers
        }
        for uri in cluster.members
    ]
    values_by_scheme: dict[str, set[str]] = defaultdict(set)
    for identifiers in identifiers_by_agent:
        for scheme, value in identifiers:
            values_by_scheme[scheme].add(value)
    risks = []
    if _has_conflicting_names(names):
        risks.append("conflicting_names")
    if any(not normalize_name(name.display) for name in names):
        risks.append("missing_name")
    if not set.intersection(*identifiers_by_agent):
        risks.append("no_common_identifier")
    for scheme, values in sorted(values_by_scheme.items()):
        if len(values) > 1:
            risks.append(f"multiple_{scheme}_values")
        if scheme == "orcid" and any(not is_valid_orcid(value) for value in values):
            risks.append("invalid_orcid")
    return risks


def select_mergeable_clusters(
    config_path: str,
    duplicate_path: str,
    report_path: str,
    merge_path: str,
    review_path: str,
    workers: int,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    paths = [
        os.path.abspath(path)
        for path in (config_path, duplicate_path, report_path, merge_path, review_path)
    ]
    if len(set(paths)) != len(paths):
        raise ValueError("Input and output paths must be distinct")
    config_path, duplicate_path, report_path, merge_path, review_path = paths
    config = load_audit_config(config_path)
    cache = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    for path in (report_path, merge_path, review_path):
        _ensure_parent(path)
    cluster_count = _count_duplicate_clusters(duplicate_path)
    mergeable_count = 0
    deferred_count = 0
    risk_counts: Counter[str] = Counter()
    with (
        TemporaryDirectory(dir=os.path.dirname(merge_path)) as merge_dir,
        TemporaryDirectory(dir=os.path.dirname(review_path)) as review_dir,
    ):
        pending_merge = os.path.join(merge_dir, "merge.csv")
        pending_review = os.path.join(review_dir, "review.csv")
        with (
            open(pending_merge, "w", newline="", encoding="utf-8") as merge_stream,
            open(pending_review, "w", newline="", encoding="utf-8") as review_stream,
            create_progress() as progress,
        ):
            merge_writer = csv.writer(merge_stream)
            review_writer = csv.writer(review_stream)
            merge_writer.writerow(MERGE_FIELDS)
            review_writer.writerow(DEFERRED_FIELDS)
            task = progress.add_task("Checking duplicate clusters", total=cluster_count)
            for batch in iter_cluster_batches(duplicate_path):
                if _stop_requested:
                    raise InterruptedError(
                        "Local selection interrupted; outputs unchanged"
                    )
                agents = load_agents(
                    {uri for cluster in batch for uri in cluster.members},
                    cache,
                    workers,
                )
                for cluster in batch:
                    risks = cluster_risks(cluster, agents)
                    row = (cluster.survivor, ";".join(cluster.members[1:]))
                    if risks:
                        review_writer.writerow((*row, ";".join(risks)))
                        deferred_count += 1
                        risk_counts.update(risks)
                    else:
                        merge_writer.writerow(row)
                        mergeable_count += 1
                progress.advance(task, len(batch))
        if _stop_requested:
            raise InterruptedError("Local selection interrupted; outputs unchanged")
        report: dict[str, object] = {
            "mode": "local_selection",
            "complete": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "config": config_path,
            "config_sha256": _sha256(config_path),
            "duplicates": duplicate_path,
            "duplicates_sha256": _sha256(duplicate_path),
            "merge_file": merge_path,
            "review_file": review_path,
            "summary": {
                "total_clusters": mergeable_count + deferred_count,
                "mergeable_clusters": mergeable_count,
                "deferred_clusters": deferred_count,
                "risk_counts": dict(sorted(risk_counts.items())),
            },
        }
        if _stop_requested:
            raise InterruptedError("Local selection interrupted; outputs unchanged")
        os.replace(pending_merge, merge_path)
        os.replace(pending_review, review_path)
        _write_json(report_path, report)
    return report


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
    batch_size = 24
    with (
        create_progress() as progress,
        ThreadPoolExecutor(max_workers=workers) as executor,
    ):
        task = progress.add_task("Scanning agent role archives", total=len(files))
        for batch_number, partial in enumerate(
            executor.map(
                lambda paths: _scan_roles_batch(paths, targets),
                _batches(files, batch_size),
            ),
            start=1,
        ):
            result.update(partial)
            progress.update(task, completed=min(batch_number * batch_size, len(files)))
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
    ra: str = "",
    identifier_uri: str = "",
    old_value: str = "",
    evidence: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "csv_row": csv_row,
        "action": action,
        "ra": ra,
        "identifier_uri": identifier_uri,
        "old_value": old_value,
        "confidence": round(confidence, 3),
        "reason": reason,
        "approved": False,
    }
    if evidence is not None:
        result["evidence"] = evidence
    result["operation_id"] = _computed_operation_id(result)
    return result


def _computed_operation_id(operation: dict[str, object]) -> str:
    evidence = operation["evidence"] if "evidence" in operation else None
    evidence_value = (
        orjson.dumps(evidence, option=orjson.OPT_SORT_KEYS).decode()
        if evidence is not None
        else ""
    )
    return _operation_id(
        *(cast(str, operation[field]) for field in ("action", "ra")),
        cast(str, operation["identifier_uri"]),
        cast(str, operation["old_value"]),
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


def iter_work_sources(
    client: WorkEvidenceClient, doi: str, openalex_id: str
) -> Iterator[WorkMetadata]:
    if doi:
        primary = client.crossref(doi)
        if _stop_requested:
            return
        if primary is None:
            primary = client.datacite(doi)
        if primary is not None:
            yield primary
    if not _stop_requested and (doi or openalex_id):
        secondary = client.openalex_work(doi, openalex_id)
        if secondary is not None:
            yield secondary


def collect_external_evidence(
    selected_works: set[str],
    works: dict[str, WorkInfo],
    roles: dict[str, RoleInfo],
    agents: dict[str, AgentInfo],
    cluster_by_ra: dict[str, Cluster],
    client: WorkEvidenceClient,
    targets: set[tuple[str, str]],
    profiles: dict[str, OrcidProfile | None],
) -> tuple[
    list[dict[str, object]],
    dict[tuple[str, str], list[WorkEvidence]],
    dict[str, list[PersonName]],
]:
    role_assessments = []
    edge_evidence: dict[tuple[str, str], list[WorkEvidence]] = defaultdict(list)
    names_by_orcid: dict[str, list[PersonName]] = defaultdict(list)
    pending = targets.copy()
    targets_by_ra: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for edge in targets:
        targets_by_ra[edge[0]].add(edge)

    with create_progress() as progress:
        task = progress.add_task("Querying work metadata", total=len(selected_works))
        for br_uri in sorted(selected_works):
            if _stop_requested or not pending:
                break
            work = works[br_uri]
            work_ras = {roles[uri].ra for uri in work.role_uris}
            needed = {
                edge
                for ra in work_ras
                if ra in targets_by_ra
                for edge in targets_by_ra[ra]
            } & pending
            needed_ras = {ra for ra, _ in needed}
            if not needed:
                progress.advance(task)
                continue
            doi = work.identifier("doi")
            openalex_id = work.identifier("openalex")
            chains = {}
            for role_name, chain in _role_chains(work, roles).items():
                if not any(role.ra in needed_ras for role in chain.roles):
                    continue
                if chain.status != "valid":
                    _, report = _alignment_report(chain, [], agents)
                    report.update(
                        {
                            "br": br_uri,
                            "role": role_name,
                            "source": "",
                            "reason": "invalid_local_chain",
                        }
                    )
                    role_assessments.append(report)
                else:
                    chains[role_name] = chain
            if not chains:
                progress.advance(task)
                continue
            found_source = False
            for source_work in iter_work_sources(client, doi, openalex_id):
                found_source = True
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
                    if alignment is None:
                        continue
                    matched = _alignment_dict(alignment)
                    for local_index, role in enumerate(chain.roles):
                        for identifier in agents[role.ra].orcids:
                            normalized = normalize_orcid(identifier.value)
                            if (role.ra, normalized) not in needed:
                                continue
                            match = matched.get(local_index)
                            if match is None:
                                continue
                            external_index, score = match
                            api_agent = external[external_index]
                            contested_elsewhere = any(
                                normalize_orcid(agent["orcid"] or "") == normalized
                                for index, agent in enumerate(external)
                                if index != external_index
                            )
                            edge_evidence[(role.ra, normalized)].append(
                                WorkEvidence(
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
                            )
                    for api_agent in external:
                        api_orcid = normalize_orcid(api_agent["orcid"] or "")
                        if api_orcid:
                            names_by_orcid[api_orcid].append(
                                _agent_metadata_name(api_agent)
                            )
                touched_clusters = {cluster_by_ra[ra] for ra, _ in needed}
                assessments, _ = classify_identifiers(
                    sorted(touched_clusters, key=lambda cluster: cluster.csv_row),
                    agents,
                    edge_evidence,
                    names_by_orcid,
                    {},
                    profiles,
                )
                pending.difference_update(
                    (cast(str, assessment["ra"]), cast(str, assessment["orcid"]))
                    for assessment in assessments
                    if assessment["status"] == "verified_wrong"
                )
                needed.intersection_update(pending)
                if not needed or _stop_requested:
                    break
            if not found_source:
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
            progress.advance(task)
    return (
        role_assessments,
        edge_evidence,
        names_by_orcid,
    )


def _polluted_identifier(names: list[PersonName]) -> bool:
    for left, right in combinations(names, 2):
        if name_score(left, right) < AMBIGUOUS_NAME_SCORE:
            return True
    return False


def load_orcid_profiles(
    clusters: list[Cluster],
    agents: dict[str, AgentInfo],
    client: OrcidClient,
) -> dict[str, OrcidProfile | None]:
    profiles: dict[str, OrcidProfile | None] = {}
    candidate_orcids = {
        normalize_orcid(identifier.value)
        for cluster in clusters
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

    return profiles


def _profile_match(
    cluster: Cluster,
    agent: AgentInfo,
    agents: dict[str, AgentInfo],
    profile: OrcidProfile | None,
) -> tuple[PersonName, float, float, str]:
    name = (
        PersonName(
            name=profile["name"], given=profile["given"], family=profile["family"]
        )
        if profile is not None
        else PersonName()
    )
    other_scores = [
        (name_score(agents[other].name, name), other)
        for other in cluster.members
        if other != agent.uri and name.display
    ]
    best_score, best_other = max(other_scores) if other_scores else (0.0, "")
    return name, name_score(agent.name, name), best_score, best_other


def work_evidence_targets(
    clusters: list[Cluster],
    agents: dict[str, AgentInfo],
    profiles: dict[str, OrcidProfile | None],
) -> set[tuple[str, str]]:
    targets = set()
    for cluster in clusters:
        for member in cluster.members:
            agent = agents[member]
            if not normalize_name(agent.name.display):
                continue
            for identifier in agent.orcids:
                orcid = normalize_orcid(identifier.value)
                if not is_valid_orcid(orcid):
                    targets.add((member, orcid))
                    continue
                profile = profiles.get(orcid)
                if profile is None:
                    continue
                profile_name, score, other_score, _ = _profile_match(
                    cluster, agent, agents, profile
                )
                if (
                    score < AMBIGUOUS_NAME_SCORE
                    and other_score >= CONFIRMED_NAME_SCORE
                    and script_family(agent.name.display)
                    == script_family(profile_name.display)
                ):
                    targets.add((member, orcid))
    return targets


def classify_identifiers(
    clusters: list[Cluster],
    agents: dict[str, AgentInfo],
    edge_evidence: dict[tuple[str, str], list[WorkEvidence]],
    names_by_orcid: dict[str, list[PersonName]],
    provenance: dict[str, dict[str, object]],
    profiles: dict[str, OrcidProfile | None],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    assessments = []
    operations = []
    for cluster in clusters:
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
                profile_name, profile_score, best_other_score, best_other = (
                    _profile_match(cluster, agent, agents, profile)
                )
                cross_script = bool(profile_name.display) and script_family(
                    agent.name.display
                ) != script_family(profile_name.display)
                polluted = _polluted_identifier(
                    names_by_orcid[orcid] if orcid in names_by_orcid else []
                )
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
                    "work_evidence": [asdict(item) for item in evidence],
                    "agent_provenance": provenance.get(member),
                    "identifier_provenance": provenance.get(identifier.uri),
                }
                assessments.append(assessment)
                if status != "verified_wrong":
                    continue
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
                        "detach_identifier",
                        cluster.csv_row,
                        reason,
                        max(best_other_score, CONFIRMED_NAME_SCORE),
                        ra=member,
                        identifier_uri=identifier.uri,
                        old_value=orcid,
                        evidence=evidence_links,
                    )
                )
    return assessments, operations


def write_review_file(path: str, operations: list[dict[str, object]]) -> None:
    _ensure_parent(path)
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=REVIEW_FIELDS)
        writer.writeheader()
        for operation in operations:
            row = {
                field: operation[field]
                for field in REVIEW_FIELDS
                if field != "decision"
            }
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
    clusters, agents, risks_by_row, cluster_count, agent_count = (
        scan_candidate_clusters(duplicate_path, cache, workers)
    )
    candidate_ras = set(agents)
    cluster_by_ra = {
        member: cluster for cluster in clusters for member in cluster.members
    }
    _ensure_parent(cache_path)
    api_cache = ApiCache(cache_path)
    client = AgentMetadataClient(
        mailto=mailto,
        cache=api_cache,
        refresh_cache=refresh_cache,
        openalex_api_key=openalex_api_key,
    )
    try:
        profiles = load_orcid_profiles(clusters, agents, client)
        targets = work_evidence_targets(clusters, agents, profiles)
        if targets and not _stop_requested:
            works, roles, contextual_agents = build_context(
                {ra for ra, _ in targets},
                config.rdf_dir,
                config.zip_output,
                cache,
                workers,
                max_evidence_works,
            )
            agents.update(contextual_agents)
        else:
            works, roles = {}, {}
        role_assessments, edge_evidence, names_by_orcid = collect_external_evidence(
            set(works),
            works,
            roles,
            agents,
            cluster_by_ra,
            client,
            targets,
            profiles,
        )
        provenance_uris = set(candidate_ras)
        provenance_uris.update(
            identifier.uri
            for ra in candidate_ras
            for identifier in agents[ra].identifiers
        )
        provenance = load_provenance(provenance_uris, cache, workers)
        identifier_assessments, identifier_operations = classify_identifiers(
            clusters,
            agents,
            edge_evidence,
            names_by_orcid,
            provenance,
            profiles,
        )
    finally:
        client.close()
        api_cache.close()
    operations_by_id = {
        cast(str, operation["operation_id"]): operation
        for operation in identifier_operations
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
            "selected_works": len(works),
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
            expected = {
                field: str(operation[field])
                for field in REVIEW_FIELDS
                if field != "decision"
            }
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


def _operation_string(operation: dict[str, object], field: str) -> str:
    value = operation[field]
    if not isinstance(value, str):
        raise ValueError(f"Operation field {field} must be a string")
    return value


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
) -> set[str]:
    uris = set()
    for operation in operations:
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
        values = _current_objects(editor.endpoint, identifier_uri, HAS_LITERAL_VALUE)
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
            if _current_objects(editor.endpoint, ar_uri, IS_HELD_BY) != [evidence_ra]:
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
    return uris


def _apply_operation_group(
    editor: MetaEditor, operations: list[dict[str, object]]
) -> None:
    uris = _preflight_operations(editor, operations)

    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=editor.supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    _import_entities(editor, g_set, uris)
    for operation in operations:
        ra = _responsible_agent(g_set, _operation_string(operation, "ra"))
        identifier_uri = _operation_string(operation, "identifier_uri")
        ra.remove_identifier(_identifier(g_set, identifier_uri))
    editor.save(g_set, editor.supplier_prefix)


def _validate_approved_operations(operations: list[dict[str, object]]) -> None:
    identifiers = set()
    for operation in operations:
        action = _operation_string(operation, "action")
        if action != "detach_identifier":
            raise ValueError(f"Unsupported approved operation: {action}")
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


def _operation_resources(operation: dict[str, object]) -> set[str]:
    resources = {
        _operation_string(operation, "ra"),
        _operation_string(operation, "identifier_uri"),
    }
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
            "Select locally consistent responsible-agent clusters for merging, "
            "or plan and apply reviewed ORCID corrections for deferred clusters."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta YAML config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Select mergeable clusters using local RDF only",
    )
    mode.add_argument(
        "--repair",
        action="store_true",
        help="Plan ORCID corrections using external evidence",
    )
    mode.add_argument("--execute", metavar="PLAN", help="Execute an approved plan")
    parser.add_argument(
        "--duplicates", help="Duplicate RA CSV produced by find.duplicates"
    )
    parser.add_argument(
        "--report-file", help="Local summary or correction plan JSON path"
    )
    parser.add_argument(
        "--merge-file", help="Local selection CSV; defaults to REPORT.merge.csv"
    )
    parser.add_argument(
        "--review-file",
        help="Deferred clusters for --dry-run; operation decisions for --repair or --execute",
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
        if not args.duplicates or not args.report_file:
            parser.error("--duplicates and --report-file are required with --dry-run")
        merge_path = args.merge_file or f"{args.report_file}.merge.csv"
        review_path = args.review_file or f"{args.report_file}.deferred.csv"
        report = select_mergeable_clusters(
            args.config,
            args.duplicates,
            args.report_file,
            merge_path,
            review_path,
            args.workers,
        )
        summary = cast(dict[str, object], report["summary"])
        console.print(
            f"Mergeable clusters: {summary['mergeable_clusters']}; "
            f"deferred clusters: {summary['deferred_clusters']}. "
            f"Merge CSV: {os.path.abspath(merge_path)}. "
            f"Deferred CSV: {os.path.abspath(review_path)}."
        )
        return
    if args.repair:
        if not args.duplicates or not args.report_file or not args.mailto:
            parser.error(
                "--duplicates, --report-file, and --mailto are required with --repair"
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
