# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
from pathlib import Path

from oc_meta.run.find.duplicated_entities import (
    find_duplicates,
    save_duplicates_to_csv,
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

    assert find_duplicates(resources, qualities) == [
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

    save_duplicates_to_csv(resources, csv_path, qualities)

    with csv_path.open(encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert rows == [
        {
            "surviving_entity": "https://w3id.org/oc/meta/ra/2",
            "merged_entities": "https://w3id.org/oc/meta/ra/1",
        }
    ]
