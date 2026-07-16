# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import hashlib
import multiprocessing
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import TypeVar, cast

import orjson
import yaml
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic.agent_role import AgentRole
from oc_ocdm.graph.entities.bibliographic.responsible_agent import ResponsibleAgent
from oc_ocdm.graph.entities.identifier import Identifier

from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.meta.generate_csv import load_json_from_file

_forkserver_context = multiprocessing.get_context("forkserver")

HAS_IDENTIFIER = "http://purl.org/spar/datacite/hasIdentifier"
USES_IDENTIFIER_SCHEME = "http://purl.org/spar/datacite/usesIdentifierScheme"
HAS_LITERAL_VALUE = (
    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
)
IS_DOCUMENT_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"
WITH_ROLE = "http://purl.org/spar/pro/withRole"
HAS_NEXT = "https://w3id.org/oc/ontology/hasNext"
FOAF_NAME = "http://xmlns.com/foaf/0.1/name"
GIVEN_NAME = "http://xmlns.com/foaf/0.1/givenName"
FAMILY_NAME = "http://xmlns.com/foaf/0.1/familyName"
PROV_SPECIALIZATION_OF = "http://www.w3.org/ns/prov#specializationOf"
DATACITE_PREFIX = "http://purl.org/spar/datacite/"
ROLE_MAP = {
    "http://purl.org/spar/pro/author": "author",
    "http://purl.org/spar/pro/editor": "editor",
    "http://purl.org/spar/pro/publisher": "publisher",
}

Item = TypeVar("Item")


@dataclass(frozen=True, slots=True)
class AuditConfig:
    rdf_dir: str
    dir_split: int
    items_per_file: int
    zip_output: bool


@dataclass(frozen=True, slots=True)
class EntityFileLocator:
    rdf_dir: str
    dir_split: int
    items_per_file: int
    zip_output: bool

    def path(self, uri: str) -> str:
        return find_rdf_file(
            uri,
            self.rdf_dir,
            self.dir_split,
            self.items_per_file,
            self.zip_output,
        )


def values(entity: dict[str, object], predicate: str, key: str) -> list[str]:
    raw = entity[predicate] if predicate in entity else []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [
        value[key]
        for value in raw
        if isinstance(value, dict) and isinstance(value.get(key), str)
    ]


def ids(entity: dict[str, object], predicate: str) -> list[str]:
    return values(entity, predicate, "@id")


def literals(entity: dict[str, object], predicate: str) -> list[str]:
    return values(entity, predicate, "@value")


def first(values: list[str]) -> str:
    return values[0] if values else ""


def batches(items: list[Item], size: int) -> list[list[Item]]:
    return [items[start : start + size] for start in range(0, len(items), size)]


def sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def write_json(path: str, value: object) -> None:
    ensure_parent(path)
    temporary_path = f"{path}.tmp"
    with open(temporary_path, "wb") as stream:
        stream.write(
            orjson.dumps(value, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
        )
        stream.write(b"\n")
    os.replace(temporary_path, path)


def read_json_object(path: str) -> dict[str, object]:
    with open(path, "rb") as stream:
        value = orjson.loads(stream.read())
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return cast(dict[str, object], value)


def load_entities(path: str) -> dict[str, dict[str, object]]:
    if path.endswith(".zip"):
        graphs = load_json_from_file(path)
    else:
        with open(path, "rb") as stream:
            graphs = orjson.loads(stream.read())
    entities = {}
    for graph in graphs:
        for raw_entity in graph["@graph"]:
            entity = cast(dict[str, object], raw_entity)
            uri = entity["@id"]
            if isinstance(uri, str):
                entities[uri] = entity
    return entities


def data_files(directory: str, zip_output: bool) -> list[str]:
    extension = ".zip" if zip_output else ".json"
    paths = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            if not filename.endswith(extension):
                continue
            path = os.path.join(root, filename)
            if "prov" not in path:
                paths.append(path)
    return sorted(paths)


def load_audit_config(path: str) -> AuditConfig:
    with open(path, encoding="utf-8") as stream:
        loaded = yaml.safe_load(stream)
    if not isinstance(loaded, dict):
        raise ValueError("Meta configuration must be a YAML mapping")
    config = cast(dict[str, object], loaded)
    output_key = "output_rdf_dir" if "output_rdf_dir" in config else "base_output_dir"
    output_dir = os.path.abspath(cast(str, config[output_key]))
    return AuditConfig(
        rdf_dir=os.path.join(output_dir, "rdf"),
        dir_split=cast(int, config["dir_split_number"]),
        items_per_file=cast(int, config["items_per_file"]),
        zip_output=cast(bool, config["zip_output_rdf"]),
    )


def _load_target_batch(
    tasks: list[tuple[str, frozenset[str]]],
) -> dict[str, dict[str, object]]:
    result = {}
    for path, targets in tasks:
        if not os.path.exists(path):
            continue
        for uri, entity in load_entities(path).items():
            if uri in targets:
                result[uri] = entity
    return result


def load_available_entities(
    uris: set[str], locator: EntityFileLocator, workers: int
) -> dict[str, dict[str, object]]:
    targets_by_path: dict[str, set[str]] = defaultdict(set)
    for uri in uris:
        targets_by_path[locator.path(uri)].add(uri)
    tasks = [(path, frozenset(targets)) for path, targets in targets_by_path.items()]
    result = {}
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=_forkserver_context,
    ) as executor:
        for partial in executor.map(_load_target_batch, batches(tasks, 24)):
            result.update(partial)
    return result


def provenance_path(data_path: str, zip_output: bool) -> str:
    stem = os.path.splitext(data_path)[0]
    extension = "zip" if zip_output else "json"
    return os.path.join(stem, "prov", f"se.{extension}")


def snapshot_number(uri: str) -> int:
    value = uri.rsplit("/", 1)[-1]
    return int(value) if value.isdigit() else 0


def load_progress(path: str, plan_sha256: str, review_sha256: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    progress = read_json_object(path)
    if progress["plan_sha256"] != plan_sha256:
        raise ValueError("Progress file belongs to a different correction plan")
    if progress["review_sha256"] != review_sha256:
        raise ValueError("Review decisions changed after execution started")
    completed = progress["completed_groups"]
    if not isinstance(completed, list) or not all(
        isinstance(group_id, str) for group_id in completed
    ):
        raise ValueError("Invalid completed_groups in progress file")
    return set(cast(list[str], completed))


def save_progress(
    path: str, plan_sha256: str, review_sha256: str, completed: set[str]
) -> None:
    write_json(
        path,
        {
            "plan_sha256": plan_sha256,
            "review_sha256": review_sha256,
            "completed_groups": sorted(completed),
        },
    )


def responsible_agent(g_set: GraphSet, uri: str) -> ResponsibleAgent:
    entity = g_set.get_entity(uri)
    if not isinstance(entity, ResponsibleAgent):
        raise ValueError(f"Responsible agent not imported: {uri}")
    return entity


def identifier(g_set: GraphSet, uri: str) -> Identifier:
    entity = g_set.get_entity(uri)
    if not isinstance(entity, Identifier):
        raise ValueError(f"Identifier not imported: {uri}")
    return entity


def agent_role(g_set: GraphSet, uri: str) -> AgentRole:
    entity = g_set.get_entity(uri)
    if not isinstance(entity, AgentRole):
        raise ValueError(f"Agent role not imported: {uri}")
    return entity
