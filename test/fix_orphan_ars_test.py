# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import csv
from pathlib import Path
from typing import cast

import pytest

from oc_meta.run.patches import fix_orphan_ars as fixer
from test.fix_appended_roles_test import (
    ORIGINAL_DOI,
    StubProvider,
    _publisher,
    _write_work_provenance,
)
from test.fix_dangling_ars_test import (
    BASE,
    AR_1,
    AR_2,
    AR_3,
    AR_4,
    BR_1,
    ID_1,
    RA_1,
    _br_entity,
    _doi_entity,
    _role_entity,
    _set_counters,
    _work,
    _write_config,
    _write_entities,
)

AR_5 = f"{BASE}ar/0605"
CONTEXT = "http://purl.org/spar/pro/isDocumentContextFor"


@pytest.fixture(autouse=True)
def _stub_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    StubProvider.records = {}
    StubProvider.calls = []
    monkeypatch.setattr(fixer, "AgentMetadataClient", StubProvider)


def _setup(tmp_path: Path, roles: list[dict[str, object]]) -> tuple[Path, Path]:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    _write_entities(
        rdf_dir,
        [
            _br_entity(BR_1, ID_1, [AR_1, AR_2]),
            _doi_entity(ID_1, ORIGINAL_DOI),
            *roles,
        ],
    )
    _set_counters(output_dir, 5, 1)
    return config_path, rdf_dir


def _analyze(tmp_path: Path, config_path: Path) -> dict[str, object]:
    return fixer.analyze_orphan_roles(
        str(config_path),
        str(tmp_path / "plan.json"),
        str(tmp_path / "csv"),
        str(tmp_path / "api.sqlite"),
        "test@example.org",
        1,
        False,
    )


def _execute(tmp_path: Path, config_path: Path) -> dict[str, object]:
    return fixer.execute_plan(
        str(config_path),
        str(tmp_path / "plan.json"),
        "https://example.org/agent",
        str(tmp_path / "progress.json"),
        str(tmp_path / "execution.json"),
        1,
    )


def test_unreferenced_roles_are_deleted(tmp_path: Path) -> None:
    config_path, rdf_dir = _setup(
        tmp_path,
        [
            _role_entity(AR_1, RA_1, AR_2),
            _role_entity(AR_2, RA_1),
            _role_entity(AR_3, RA_1, AR_4),
            _role_entity(AR_4, RA_1, AR_1),
            {"@id": AR_5, fixer.HAS_NEXT: [{"@id": AR_5}]},
        ],
    )

    report = _analyze(tmp_path, config_path)
    execution = _execute(tmp_path, config_path)

    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    roles = fixer.load_available_entities({AR_1, AR_2, AR_3, AR_4, AR_5}, locator, 1)
    operations = cast(list[dict[str, object]], report["operations"])
    assert report["summary"] == {
        "existing_roles": 5,
        "referenced_roles": 2,
        "orphan_roles": 3,
        "orphan_role_types": {"author": 2},
        "orphan_roles_without_role_type": 1,
        "links_from_orphan_to_referenced_roles": 1,
        "works_with_lost_roles": 0,
        "reingested_works": 0,
        "blocked_works": 0,
        "blockers": {},
        "providers": {},
        "kept_orphan_roles": 0,
        "operations": 1,
    }
    assert [sorted(cast(dict[str, str], op["roles"])) for op in operations] == [
        [AR_3, AR_4, AR_5]
    ]
    assert execution["complete"] is True
    assert sorted(roles) == [AR_1, AR_2]
    assert fixer.ids(roles[AR_1], fixer.HAS_NEXT) == [AR_2]
    assert Path(cast(str, execution["reindex_sentinel"])).exists()


def test_links_from_referenced_roles_to_orphans_stop_the_dry_run(
    tmp_path: Path,
) -> None:
    config_path, _ = _setup(
        tmp_path,
        [
            _role_entity(AR_1, RA_1, AR_2),
            _role_entity(AR_2, RA_1, AR_3),
            _role_entity(AR_3, RA_1),
        ],
    )

    with pytest.raises(
        RuntimeError,
        match=f"Referenced agent roles link to orphan roles: {AR_2} -> {AR_3}$",
    ):
        _analyze(tmp_path, config_path)


def test_execution_rejects_changed_orphans(tmp_path: Path) -> None:
    config_path, rdf_dir = _setup(
        tmp_path,
        [
            _role_entity(AR_1, RA_1, AR_2),
            _role_entity(AR_2, RA_1),
            _role_entity(AR_3, RA_1),
        ],
    )
    _analyze(tmp_path, config_path)
    _write_entities(
        rdf_dir,
        [
            _role_entity(AR_1, RA_1, AR_2),
            _role_entity(AR_2, RA_1),
            _role_entity(AR_3, RA_1, AR_4),
        ],
    )

    with pytest.raises(RuntimeError, match=f"local RDF state changed for {AR_3}$"):
        _execute(tmp_path, config_path)


def _lost_publisher(tmp_path: Path) -> tuple[Path, Path]:
    config_path, rdf_dir = _setup(
        tmp_path,
        [
            _role_entity(AR_1, RA_1, AR_2),
            _role_entity(AR_2, RA_1),
            _publisher(AR_3, RA_1),
        ],
    )
    _write_work_provenance(
        rdf_dir,
        f"INSERT DATA {{ GRAPH <{BASE}br/> {{ <{BR_1}> <{CONTEXT}> <{AR_3}> . }} }}",
    )
    return config_path, rdf_dir


def test_roles_lost_by_a_live_work_are_planned_for_reingestion(
    tmp_path: Path,
) -> None:
    config_path, rdf_dir = _lost_publisher(tmp_path)
    StubProvider.records[("crossref", ORIGINAL_DOI)] = _work(
        "crossref", publisher="Provider publisher"
    )

    report = _analyze(tmp_path, config_path)
    execution = _execute(tmp_path, config_path)

    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    work = fixer.load_entities(locator.path(BR_1))[BR_1]
    roles = fixer.load_available_entities({AR_1, AR_2, AR_3}, locator, 1)
    with (tmp_path / "csv" / "crossref" / "input.csv").open(encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    operations = cast(list[dict[str, object]], report["operations"])
    assert [
        (op["br"], op["role_types"], op["lost_roles"]) for op in operations[:1]
    ] == [(BR_1, {"publisher": []}, [AR_3])]
    assert [sorted(cast(dict[str, str], op["roles"])) for op in operations[1:]] == [
        [AR_3]
    ]
    assert [(row["id"], row["publisher"]) for row in rows] == [
        (f"omid:br/0601 doi:{ORIGINAL_DOI}", "Provider publisher")
    ]
    assert execution["complete"] is True
    assert sorted(fixer.ids(work, CONTEXT)) == [AR_1, AR_2]
    assert sorted(roles) == [AR_1, AR_2]


def test_lost_roles_are_kept_when_the_work_cannot_be_reingested(
    tmp_path: Path,
) -> None:
    config_path, _ = _lost_publisher(tmp_path)

    report = _analyze(tmp_path, config_path)

    summary = cast(dict[str, object], report["summary"])
    blocked = cast(list[dict[str, object]], report["blocked"])
    assert report["operations"] == []
    assert [(entry["blockers"], entry["lost_roles"]) for entry in blocked] == [
        (["provider_unavailable"], [AR_3])
    ]
    assert summary["kept_orphan_roles"] == 1
