# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import random
import zipfile
from pathlib import Path

import pytest

from oc_meta.run.find import duplicates
from oc_meta.run.find.duplicates import (
    ERROR_LOG_FILENAME,
    find_duplicate_brs,
    find_duplicate_ras,
)


def test_find_duplicate_brs_removes_empty_error_log(tmp_path: Path) -> None:
    rdf_dir = tmp_path / "rdf"
    output_dir = tmp_path / "output"
    csv_path = output_dir / "duplicates.csv"
    (rdf_dir / "br").mkdir(parents=True)
    output_dir.mkdir()

    find_duplicate_brs(rdf_dir, csv_path)

    assert sorted(path.name for path in output_dir.iterdir()) == ["duplicates.csv"]
    assert csv_path.read_text(encoding="utf-8") == "surviving_entity,merged_entities\n"


def test_find_duplicate_ras_writes_error_log_next_to_output(
    tmp_path: Path, monkeypatch
) -> None:
    rdf_dir = tmp_path / "rdf"
    output_dir = tmp_path / "output"
    work_dir = tmp_path / "work"
    csv_path = output_dir / "duplicates.csv"
    rdf_dir.mkdir()
    output_dir.mkdir()
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)

    find_duplicate_ras(rdf_dir, csv_path)

    assert sorted(path.name for path in output_dir.iterdir()) == [
        "duplicates.csv",
        ERROR_LOG_FILENAME,
    ]
    assert sorted(path.name for path in work_dir.iterdir()) == []


@pytest.mark.parametrize("entity_type", ["ra", "br"])
def test_disk_scan_preserves_transitive_groups_and_survivors(
    tmp_path: Path, monkeypatch, entity_type: str
) -> None:
    rdf_dir = tmp_path / "rdf"
    entity_dir = rdf_dir / entity_type
    entity_dir.mkdir(parents=True)
    prefix = f"https://w3id.org/oc/meta/{entity_type}/"
    identifier_prefix = "https://w3id.org/oc/meta/id/"
    name_property = (
        "http://xmlns.com/foaf/0.1/name"
        if entity_type == "ra"
        else "http://purl.org/dc/terms/title"
    )
    rdf_type = (
        "http://xmlns.com/foaf/0.1/Agent"
        if entity_type == "ra"
        else "http://purl.org/spar/fabio/Expression"
    )
    records = [
        [("b", ["1"], "B" * 80), ("a", ["1"], "A")],
        [("c", ["2"], "Longest contributor name"), ("d", ["2"], "D")],
        [("f", ["3"], "Equal"), ("e", ["3"], "Equal")],
        [("b", ["2", "2"], "B")],
        [("b", [], ""), ("alone", [], "No identifier"), ("unique", ["4"], "Unique")],
    ]
    source_bytes = {}
    for index, batch in enumerate(records):
        entities = []
        for suffix, identifiers, name in batch:
            entity = {
                "@id": prefix + suffix,
                "@type": [rdf_type],
                name_property: [{"@value": name}],
                "http://purl.org/spar/datacite/hasIdentifier": [
                    {"@id": identifier_prefix + identifier}
                    for identifier in identifiers
                ],
            }
            if suffix == "c" and entity_type == "br":
                entity[
                    "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
                ] = [{"@value": "2026-09-15"}]
            entities.append(entity)
        path = entity_dir / f"{index}.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("entities.json", json.dumps([{"@graph": entities}]))
        source_bytes[path] = path.read_bytes()
    with zipfile.ZipFile(entity_dir / "se.zip", "w") as archive:
        archive.writestr("se.json", "not a data file")

    initial_batch_sizes = []
    analyze = duplicates.analyze_entity_json

    def record_batch(data, local_resources, local_qualities, *args):
        initial_batch_sizes.append((len(local_resources), len(local_qualities)))
        return analyze(data, local_resources, local_qualities, *args)

    monkeypatch.setattr(duplicates, "analyze_entity_json", record_batch)
    output = tmp_path / "duplicates.csv"
    duplicates.find_duplicate_resources_by_type(rdf_dir, output, entity_type)

    assert initial_batch_sizes == [(0, 0)] * len(records)
    assert output.read_text() == (
        "surviving_entity,merged_entities\n"
        f"{prefix}c,{prefix}a; {prefix}b; {prefix}d\n"
        f"{prefix}e,{prefix}f\n"
    )
    assert {path: path.read_bytes() for path in source_bytes} == source_bytes
    assert sorted(path.name for path in tmp_path.iterdir()) == ["duplicates.csv", "rdf"]


def test_disk_scan_propagates_storage_failure_and_cleans_temporary_files(
    tmp_path: Path, monkeypatch
) -> None:
    (tmp_path / "rdf" / "ra").mkdir(parents=True)

    def fail_scan(*args):
        raise OSError("disk full")

    monkeypatch.setattr(duplicates, "process_entity_folder", fail_scan)
    with pytest.raises(OSError, match="^disk full$"):
        find_duplicate_ras(tmp_path / "rdf", tmp_path / "duplicates.csv")
    assert sorted(path.name for path in tmp_path.iterdir()) == ["rdf"]


def test_disk_scan_matches_reference_with_cycles_and_multiple_components(
    tmp_path: Path,
) -> None:
    entity_dir = tmp_path / "rdf" / "ra"
    entity_dir.mkdir(parents=True)
    random_source = random.Random(19)
    resources = {}
    qualities = {}
    for batch in range(12):
        entities = []
        for offset in range(30):
            number = batch * 30 + offset
            uri = f"https://w3id.org/oc/meta/ra/{number}"
            identifiers = {
                f"https://w3id.org/oc/meta/id/{number % 3}/{value}"
                for value in random_source.sample(range(40), 2)
            }
            entity = {
                "@id": uri,
                "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                "http://xmlns.com/foaf/0.1/name": [{"@value": "A" * (number % 7 + 1)}],
                "http://purl.org/spar/datacite/hasIdentifier": [
                    {"@id": identifier} for identifier in sorted(identifiers)
                ],
            }
            resources[uri] = identifiers
            qualities[uri] = duplicates.get_entity_quality(entity, "ra")
            entities.append(entity)
        with zipfile.ZipFile(entity_dir / f"{batch:02}.zip", "w") as archive:
            archive.writestr("entities.json", json.dumps([{"@graph": entities}]))
    expected = tmp_path / "expected.csv"
    remaining = set(resources)
    expected_groups = []
    for seed in resources:
        if seed not in remaining:
            continue
        remaining.remove(seed)
        group = {seed}
        pending = [seed]
        while pending:
            current = pending.pop()
            neighbors = {
                uri for uri in remaining if resources[current] & resources[uri]
            }
            remaining.difference_update(neighbors)
            group.update(neighbors)
            pending.extend(neighbors)
        if len(group) > 1:
            survivor = min(
                group, key=lambda uri: (*(-value for value in qualities[uri]), uri)
            )
            expected_groups.append((survivor, sorted(group - {survivor})))
    duplicates.save_merge_rows_to_csv(expected_groups, expected)
    actual = tmp_path / "actual.csv"
    find_duplicate_ras(tmp_path / "rdf", actual)
    assert actual.read_bytes() == expected.read_bytes()


def test_disk_scan_keeps_existing_csv_if_grouping_fails(
    tmp_path: Path, monkeypatch
) -> None:
    (tmp_path / "rdf" / "ra").mkdir(parents=True)
    output = tmp_path / "duplicates.csv"
    output.write_text("previous result\n")

    def fail_groups(connection):
        yield "ra/1", ["ra/2"]
        raise OSError("disk full")

    monkeypatch.setattr(duplicates, "disk_duplicate_groups", fail_groups)
    with pytest.raises(OSError, match="^disk full$"):
        find_duplicate_ras(tmp_path / "rdf", output)
    assert output.read_text() == "previous result\n"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["duplicates.csv", "rdf"]
