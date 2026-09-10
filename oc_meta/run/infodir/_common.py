# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import multiprocessing
import os
import struct
import zipfile
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from itertools import islice
from typing import BinaryIO, TypeVar

import orjson
from oc_ocdm.support import get_prefix, get_resource_number, get_short_name

from oc_meta.lib.console import advance_progress, create_progress
from oc_meta.lib.file_manager import collect_zip_files

ENTITY_TYPES = frozenset({"ar", "br", "ra", "re", "id"})
COUNTER_SIZE = 4
COUNTERS_PER_CHUNK = 100_000

CounterKey = tuple[str, str]
ResultType = TypeVar("ResultType")


class SourceDataError(ValueError):
    pass


@dataclass(frozen=True)
class DataEntity:
    uri: str
    resource_number: int


@dataclass(frozen=True)
class DataZipResult:
    path: str
    entities: dict[CounterKey, list[DataEntity]]


@dataclass(frozen=True)
class ProvenanceZipResult:
    path: str
    counters: dict[CounterKey, dict[int, int]]


@dataclass(frozen=True)
class DataScanResult:
    zip_files: int
    entities: int
    maxima: dict[CounterKey, int]
    missing_provenance: int
    missing_examples: list[dict[str, object]]


@dataclass(frozen=True)
class ProvenanceScanResult:
    zip_files: int
    entities: int


class SparseCounterStore:
    def __init__(self, root: str) -> None:
        self.root = root
        self.maxima: dict[CounterKey, int] = {}
        self._files: dict[CounterKey, BinaryIO] = {}

    def keys(self) -> set[CounterKey]:
        return set(self.maxima)

    def maximum(self, key: CounterKey) -> int:
        return self.maxima[key] if key in self.maxima else 0

    def write_updates(self, key: CounterKey, updates: dict[int, int]) -> None:
        if not updates:
            return
        ordered = sorted(updates.items())
        run: list[tuple[int, int]] = []
        previous_resource = 0
        for resource_number, counter in ordered:
            if counter > 2**32 - 1:
                raise SourceDataError(
                    f"Snapshot counter {counter} exceeds the supported range"
                )
            if run and resource_number != previous_resource + 1:
                self._write_run(key, run)
                run = []
            run.append((resource_number, counter))
            previous_resource = resource_number
        self._write_run(key, run)
        self.maxima[key] = max(self.maximum(key), ordered[-1][0])

    def read_span(self, key: CounterKey, start: int, end: int) -> list[int]:
        if end < start:
            return []
        size = end - start + 1
        file_handle = self._files[key] if key in self._files else None
        if file_handle is None:
            return [0] * size
        file_handle.seek((start - 1) * COUNTER_SIZE)
        data = file_handle.read(size * COUNTER_SIZE)
        if len(data) < size * COUNTER_SIZE:
            data += b"\0" * (size * COUNTER_SIZE - len(data))
        return list(struct.unpack(f"<{size}I", data))

    def iter_chunks(
        self, key: CounterKey, start: int = 1, end: int | None = None
    ) -> Iterator[tuple[int, list[int]]]:
        maximum = self.maximum(key) if end is None else end
        current = start
        while current <= maximum:
            chunk_end = min(current + COUNTERS_PER_CHUNK - 1, maximum)
            yield current, self.read_span(key, current, chunk_end)
            current = chunk_end + 1

    def render(self, key: CounterKey, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        rendered: dict[int, bytes] = {0: b"\n"}
        with open(output_path, "wb") as output_file:
            for _, counters in self.iter_chunks(key):
                lines: list[bytes] = []
                for counter in counters:
                    if counter not in rendered:
                        rendered[counter] = f"{counter}\n".encode()
                    lines.append(rendered[counter])
                output_file.writelines(lines)

    def close(self) -> None:
        for file_handle in self._files.values():
            file_handle.close()
        self._files.clear()

    def _write_run(self, key: CounterKey, run: list[tuple[int, int]]) -> None:
        start = run[0][0]
        current = self.read_span(key, start, run[-1][0])
        merged = [
            max(persisted, update[1])
            for persisted, update in zip(current, run, strict=True)
        ]
        file_handle = self._file(key)
        file_handle.seek((start - 1) * COUNTER_SIZE)
        file_handle.write(struct.pack(f"<{len(merged)}I", *merged))

    def _file(self, key: CounterKey) -> BinaryIO:
        if key not in self._files:
            prefix, short_name = key
            directory = os.path.join(self.root, prefix)
            os.makedirs(directory, exist_ok=True)
            self._files[key] = open(os.path.join(directory, f"{short_name}.bin"), "w+b")
        return self._files[key]


def bounded_process_map(
    paths: Iterable[str],
    worker: Callable[[str], ResultType],
    workers: int,
) -> Iterator[ResultType]:
    if workers <= 0:
        raise ValueError("workers must be greater than zero")
    path_iterator = iter(paths)
    context = multiprocessing.get_context("spawn") if os.name == "nt" else multiprocessing.get_context("forkserver")
    pending: dict[Future[ResultType], str] = {}
    with ProcessPoolExecutor(max_workers=workers, mp_context=context) as executor:
        for path in islice(path_iterator, workers * 2):
            pending[executor.submit(worker, path)] = path
        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                pending.pop(future)
                yield future.result()
                try:
                    path = next(path_iterator)
                except StopIteration:
                    continue
                pending[executor.submit(worker, path)] = path


def process_data_zip(path: str) -> DataZipResult:
    entities: dict[CounterKey, list[DataEntity]] = {}
    for entity in _graph_entities(path):
        uri = _entity_uri(entity, path)
        key, resource_number = _counter_identity(uri, path)
        if key not in entities:
            entities[key] = []
        entities[key].append(DataEntity(uri=uri, resource_number=resource_number))
    for values in entities.values():
        values.sort(key=lambda entity: entity.resource_number)
    return DataZipResult(path=path, entities=entities)


def process_provenance_zip(path: str) -> ProvenanceZipResult:
    counters: dict[CounterKey, dict[int, int]] = {}
    for entity in _graph_entities(path):
        snapshot_uri = _entity_uri(entity, path)
        parts = snapshot_uri.rsplit("/prov/se/", 1)
        if len(parts) != 2:
            raise SourceDataError(
                f"Invalid provenance entity URI in {path}: {snapshot_uri}"
            )
        entity_uri, snapshot_text = parts
        try:
            snapshot_number = int(snapshot_text)
        except ValueError as error:
            raise SourceDataError(
                f"Invalid snapshot number in {path}: {snapshot_uri}"
            ) from error
        if snapshot_number <= 0:
            raise SourceDataError(f"Invalid snapshot number in {path}: {snapshot_uri}")
        key, resource_number = _counter_identity(entity_uri, path)
        if key not in counters:
            counters[key] = {}
        current = (
            counters[key][resource_number] if resource_number in counters[key] else 0
        )
        counters[key][resource_number] = max(current, snapshot_number)
    return ProvenanceZipResult(path=path, counters=counters)


def scan_provenance(
    root: str, store: SparseCounterStore, workers: int
) -> ProvenanceScanResult:
    zip_files = 0
    entities = 0
    paths = collect_zip_files(root, only_prov=True)
    with create_progress() as progress:
        task_id = progress.add_task("Scanning provenance ZIP files", total=len(paths))
        for result in bounded_process_map(paths, process_provenance_zip, workers):
            zip_files += 1
            for key, updates in result.counters.items():
                store.write_updates(key, updates)
                entities += len(updates)
            advance_progress(progress, task_id)
    return ProvenanceScanResult(zip_files=zip_files, entities=entities)


def scan_data(
    root: str,
    store: SparseCounterStore,
    workers: int,
    max_examples: int,
) -> DataScanResult:
    zip_files = 0
    entities = 0
    maxima: dict[CounterKey, int] = {}
    missing_provenance = 0
    missing_examples: list[dict[str, object]] = []
    paths = collect_zip_files(root, only_data=True)
    with create_progress() as progress:
        task_id = progress.add_task("Scanning data ZIP files", total=len(paths))
        for result in bounded_process_map(paths, process_data_zip, workers):
            zip_files += 1
            for key, records in result.entities.items():
                entities += len(records)
                maxima[key] = max(
                    maxima[key] if key in maxima else 0,
                    records[-1].resource_number,
                )
                start = records[0].resource_number
                counters = store.read_span(key, start, records[-1].resource_number)
                for record in records:
                    if counters[record.resource_number - start] != 0:
                        continue
                    missing_provenance += 1
                    if len(missing_examples) < max_examples:
                        missing_examples.append(
                            {
                                "entity_uri": record.uri,
                                "zip_file": result.path,
                            }
                        )
            advance_progress(progress, task_id)
    return DataScanResult(
        zip_files=zip_files,
        entities=entities,
        maxima=maxima,
        missing_provenance=missing_provenance,
        missing_examples=missing_examples,
    )


def write_json(path: str, data: dict[str, object]) -> None:
    absolute_path = os.path.abspath(path)
    directory = os.path.dirname(absolute_path)
    os.makedirs(directory, exist_ok=True)
    temporary_path = f"{absolute_path}.tmp"
    with open(temporary_path, "wb") as output_file:
        output_file.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
        output_file.write(b"\n")
    os.replace(temporary_path, absolute_path)


def _graph_entities(path: str) -> Iterator[dict[str, object]]:
    try:
        with zipfile.ZipFile(path) as archive:
            json_names = [
                name
                for name in archive.namelist()
                if name.endswith(".json") and not name.endswith("/")
            ]
            if len(json_names) != 1:
                raise SourceDataError(
                    f"Expected one JSON member in {path}, found {len(json_names)}"
                )
            try:
                payload = orjson.loads(archive.read(json_names[0]))
            except orjson.JSONDecodeError as error:
                raise SourceDataError(f"Invalid JSON in {path}") from error
    except zipfile.BadZipFile as error:
        raise SourceDataError(f"Invalid ZIP file: {path}") from error

    if not isinstance(payload, list):
        raise SourceDataError(f"Expected a JSON array in {path}")
    for graph in payload:
        if not isinstance(graph, dict) or "@graph" not in graph:
            raise SourceDataError(f"Invalid JSON-LD graph in {path}")
        graph_entities = graph["@graph"]
        if not isinstance(graph_entities, list):
            raise SourceDataError(f"Invalid JSON-LD graph in {path}")
        for entity in graph_entities:
            if not isinstance(entity, dict):
                raise SourceDataError(f"Invalid JSON-LD entity in {path}")
            yield entity


def _entity_uri(entity: dict[str, object], path: str) -> str:
    if "@id" not in entity or not isinstance(entity["@id"], str):
        raise SourceDataError(f"JSON-LD entity without a valid @id in {path}")
    return entity["@id"]


def _counter_identity(uri: str, path: str) -> tuple[CounterKey, int]:
    prefix = get_prefix(uri)
    short_name = get_short_name(uri)
    resource_number = get_resource_number(uri)
    if not prefix or short_name not in ENTITY_TYPES or resource_number <= 0:
        raise SourceDataError(f"Invalid OpenCitations Meta entity URI in {path}: {uri}")
    return (prefix, short_name), resource_number
