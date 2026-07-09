# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
from pathlib import Path

from oc_meta.run.find.duplicates import (
    ERROR_LOG_FILENAME,
    find_duplicate_brs,
    find_duplicate_ras,
    find_entity_duplicates,
    save_entity_duplicates_to_csv,
)


def test_find_duplicates_keeps_highest_quality_entity_as_survivor() -> None:
    resources = {
        "https://w3id.org/oc/meta/br/1": {"https://w3id.org/oc/meta/id/1"},
        "https://w3id.org/oc/meta/br/2": {"https://w3id.org/oc/meta/id/1"},
    }
    qualities = {
        "https://w3id.org/oc/meta/br/1": (1, 4, 1),
        "https://w3id.org/oc/meta/br/2": (1, 10, 1),
    }

    assert find_entity_duplicates(resources, qualities) == [
        (
            "https://w3id.org/oc/meta/br/2",
            ["https://w3id.org/oc/meta/br/1"],
        )
    ]


def test_save_duplicates_to_csv_uses_quality_survivor(tmp_path: Path) -> None:
    resources = {
        "https://w3id.org/oc/meta/ra/1": {"https://w3id.org/oc/meta/id/1"},
        "https://w3id.org/oc/meta/ra/2": {"https://w3id.org/oc/meta/id/1"},
    }
    qualities = {
        "https://w3id.org/oc/meta/ra/1": (1, 1, 10),
        "https://w3id.org/oc/meta/ra/2": (1, 3, 20),
    }
    csv_path = tmp_path / "duplicates.csv"

    save_entity_duplicates_to_csv(resources, csv_path, qualities)

    with csv_path.open(encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert rows == [
        {
            "surviving_entity": "https://w3id.org/oc/meta/ra/2",
            "merged_entities": "https://w3id.org/oc/meta/ra/1",
        }
    ]


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
