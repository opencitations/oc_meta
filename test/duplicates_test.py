# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from pathlib import Path

import pytest

from oc_meta.run.find.duplicates import build_argument_parser, save_merge_rows_to_csv


def test_save_merge_rows_to_csv_writes_exact_contents(tmp_path: Path) -> None:
    csv_path = tmp_path / "duplicates.csv"

    save_merge_rows_to_csv(
        [("https://w3id.org/oc/meta/br/1", ["https://w3id.org/oc/meta/br/2"])],
        csv_path,
    )

    assert (
        csv_path.read_text(encoding="utf-8") == "surviving_entity,merged_entities\n"
        "https://w3id.org/oc/meta/br/1,https://w3id.org/oc/meta/br/2\n"
    )


def test_argument_parser_accepts_only_ids_ras_and_brs() -> None:
    parser = build_argument_parser()

    assert parser.parse_args(["ids", "rdf", "ids.csv"]).command == "ids"
    assert parser.parse_args(["ras", "rdf", "ras.csv"]).command == "ras"
    assert parser.parse_args(["brs", "rdf", "brs.csv"]).command == "brs"

    with pytest.raises(SystemExit):
        parser.parse_args(["entities", "rdf", "entities.csv"])
    with pytest.raises(SystemExit):
        parser.parse_args(["all", "rdf", "all.csv"])
    with pytest.raises(SystemExit):
        parser.parse_args(["both", "rdf", "both.csv"])
