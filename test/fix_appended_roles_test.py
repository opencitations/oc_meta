# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import csv
from pathlib import Path
from typing import cast

import orjson
import pytest

from oc_meta.lib.agent_metadata import ApiCache, WorkMetadata
from oc_meta.run.patches import fix_appended_roles as fixer
from test.fix_dangling_ars_test import (
    AR_1,
    AR_2,
    AR_3,
    AR_4,
    BASE,
    BR_1,
    ID_1,
    ID_2,
    RA_1,
    _agent,
    _br_entity,
    _doi_entity,
    _role_entity,
    _set_counters,
    _work,
    _write_config,
    _write_entities,
)

AR_5 = f"{BASE}ar/0605"
RA_2 = f"{BASE}ra/0602"
CONTEXT = "http://purl.org/spar/pro/isDocumentContextFor"
ORIGINAL_DOI = "10.1000/original"
ADDED_DOI = "10.1000/added"


class StubProvider:
    records: dict[tuple[str, str], WorkMetadata] = {}
    calls: list[tuple[str, str]] = []

    def __init__(self, mailto: str, cache: ApiCache, refresh_cache: bool) -> None:
        assert mailto == "test@example.org"
        assert refresh_cache is False

    def crossref(self, doi: str) -> WorkMetadata | None:
        self.calls.append(("crossref", doi))
        key = ("crossref", doi)
        return self.records[key] if key in self.records else None

    def datacite(self, doi: str) -> WorkMetadata | None:
        self.calls.append(("datacite", doi))
        key = ("datacite", doi)
        return self.records[key] if key in self.records else None

    def close(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _stub_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    StubProvider.records = {}
    StubProvider.calls = []
    monkeypatch.setattr(fixer, "AgentMetadataClient", StubProvider)


def _merge_query(roles: list[str]) -> str:
    triples = "".join(f"<{BR_1}> <{CONTEXT}> <{role}> ." for role in roles)
    triples += f"<{BR_1}> <{fixer.HAS_IDENTIFIER}> <{ID_2}> ."
    return (
        f"DELETE DATA {{ GRAPH <{BASE}br/> {{ "
        f'<{BR_1}> <http://purl.org/dc/terms/title> "Old; INSERT" . }} }}; '
        f"INSERT DATA {{ GRAPH <{BASE}br/> {{ {triples} }} }}"
    )


def _write_work_provenance(rdf_dir: Path, update_query: str) -> None:
    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    path = Path(fixer.provenance_path(locator.path(BR_1), False))
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshots = []
    for number, query in enumerate(["", update_query], start=1):
        snapshot: dict[str, object] = {
            "@id": f"{BR_1}/prov/se/{number}",
            "@type": ["http://www.w3.org/ns/prov#Entity"],
            "http://www.w3.org/ns/prov#specializationOf": [{"@id": BR_1}],
            fixer.GENERATED_AT: [{"@value": f"2024-01-0{number}T10:00:00+00:00"}],
        }
        if query:
            snapshot[fixer.UPDATE_QUERY] = [{"@value": query}]
        snapshots.append(snapshot)
    path.write_bytes(orjson.dumps([{"@id": f"{BR_1}/prov/", "@graph": snapshots}]))


def _publisher(uri: str, holder: str) -> dict[str, object]:
    role = _role_entity(uri, holder)
    role["http://purl.org/spar/pro/withRole"] = [
        {"@id": "http://purl.org/spar/pro/publisher"}
    ]
    return role


def _setup(
    tmp_path: Path, roles: list[dict[str, object]], appended: list[str]
) -> tuple[Path, Path]:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    work = _br_entity(BR_1, ID_1, [str(role["@id"]) for role in roles])
    work[fixer.HAS_IDENTIFIER] = [{"@id": ID_1}, {"@id": ID_2}]
    _write_entities(
        rdf_dir,
        [
            work,
            _doi_entity(ID_1, ORIGINAL_DOI),
            _doi_entity(ID_2, ADDED_DOI),
            *roles,
        ],
    )
    _write_work_provenance(rdf_dir, _merge_query(appended))
    _set_counters(output_dir, 5, 2)
    return config_path, rdf_dir


def _analyze(tmp_path: Path, config_path: Path) -> dict[str, object]:
    return fixer.analyze_appended_roles(
        str(config_path),
        str(tmp_path / "plan.json"),
        str(tmp_path / "csv"),
        str(tmp_path / "api.sqlite"),
        "test@example.org",
        1,
        False,
    )


def _execute(tmp_path: Path, config_path: Path, plan_path: Path) -> dict[str, object]:
    return fixer.execute_plan(
        str(config_path),
        str(plan_path),
        "https://example.org/agent",
        str(tmp_path / "progress.json"),
        str(tmp_path / "execution.json"),
        1,
    )


DOUBLED_AUTHORS = [
    _role_entity(AR_1, RA_1, AR_2),
    _role_entity(AR_2, RA_2, AR_3),
    _role_entity(AR_3, RA_1, AR_4),
    _role_entity(AR_4, RA_2),
    _publisher(AR_5, RA_1),
]


def test_doubled_type_is_removed_and_planned_from_the_original_doi(
    tmp_path: Path,
) -> None:
    config_path, rdf_dir = _setup(tmp_path, DOUBLED_AUTHORS, [AR_3, AR_4])
    StubProvider.records[("crossref", ORIGINAL_DOI)] = _work(
        "crossref",
        authors=[
            _agent("", "author", 1, (("orcid", "0000-0002-1825-0097"),), "Doe", "Jane")
        ],
        publisher="Provider publisher",
    )

    report = _analyze(tmp_path, config_path)
    execution = _execute(tmp_path, config_path, tmp_path / "plan.json")

    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    work = fixer.load_entities(locator.path(BR_1))[BR_1]
    roles = fixer.load_available_entities({AR_1, AR_2, AR_3, AR_4, AR_5}, locator, 1)
    with (tmp_path / "csv" / "crossref" / "input.csv").open(encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    operation = cast(list[dict[str, object]], report["operations"])[0]
    assert StubProvider.calls == [("crossref", ORIGINAL_DOI)]
    assert operation["doi"] == ORIGINAL_DOI
    assert operation["provider"] == "crossref"
    assert operation["role_types"] == {"author": [AR_1, AR_2, AR_3, AR_4]}
    assert rows == [
        {
            "id": f"omid:br/0601 doi:{ORIGINAL_DOI}",
            "title": "Local title",
            "author": "Doe, Jane [orcid:0000-0002-1825-0097]",
            "pub_date": "",
            "venue": "",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "journal article",
            "publisher": "",
            "editor": "",
        }
    ]
    assert execution["complete"] is True
    assert fixer.ids(work, CONTEXT) == [AR_5]
    assert sorted(roles) == [AR_5]
    assert Path(cast(str, execution["reindex_sentinel"])).exists()


def test_other_dois_are_tried_when_the_original_has_no_record(
    tmp_path: Path,
) -> None:
    config_path, _ = _setup(tmp_path, DOUBLED_AUTHORS, [AR_3, AR_4])
    StubProvider.records[("datacite", ADDED_DOI)] = _work(
        "datacite", authors=[_agent("Doe, Jane", "author", 1)]
    )

    report = _analyze(tmp_path, config_path)

    operation = cast(list[dict[str, object]], report["operations"])[0]
    assert StubProvider.calls == [
        ("crossref", ORIGINAL_DOI),
        ("datacite", ORIGINAL_DOI),
        ("crossref", ADDED_DOI),
        ("datacite", ADDED_DOI),
    ]
    assert (operation["doi"], operation["provider"]) == (ADDED_DOI, "datacite")


@pytest.mark.parametrize(
    ("record", "blocker"),
    [(None, "provider_unavailable"), (_work("crossref"), "provider_missing_role_type")],
)
def test_works_without_usable_provider_data_are_blocked(
    tmp_path: Path, record: WorkMetadata | None, blocker: str
) -> None:
    config_path, _ = _setup(tmp_path, DOUBLED_AUTHORS, [AR_3, AR_4])
    if record is not None:
        StubProvider.records[("crossref", ORIGINAL_DOI)] = record

    report = _analyze(tmp_path, config_path)

    blocked = cast(list[dict[str, object]], report["blocked"])
    assert report["operations"] == []
    assert [entry["blockers"] for entry in blocked] == [[blocker]]


def test_roles_of_another_type_are_not_doubled(tmp_path: Path) -> None:
    config_path, _ = _setup(
        tmp_path,
        [
            _role_entity(AR_1, RA_1, AR_2),
            _role_entity(AR_2, RA_2),
            _publisher(AR_5, RA_1),
        ],
        [AR_5],
    )

    report = _analyze(tmp_path, config_path)

    assert report["operations"] == []
    assert report["blocked"] == []
    assert StubProvider.calls == []


def test_execution_rejects_changed_rdf_and_modified_plans(tmp_path: Path) -> None:
    config_path, rdf_dir = _setup(tmp_path, DOUBLED_AUTHORS, [AR_3, AR_4])
    StubProvider.records[("crossref", ORIGINAL_DOI)] = _work(
        "crossref", authors=[_agent("Doe, Jane", "author", 1)]
    )
    plan_path = tmp_path / "plan.json"
    _analyze(tmp_path, config_path)
    plan = orjson.loads(plan_path.read_bytes())
    plan["operations"][0]["role_types"]["author"] = [AR_1]
    modified_path = tmp_path / "modified.json"
    modified_path.write_bytes(orjson.dumps(plan))
    _write_entities(rdf_dir, [*DOUBLED_AUTHORS[:3], _role_entity(AR_4, RA_1)])

    with pytest.raises(ValueError, match="modified operation"):
        _execute(tmp_path, config_path, modified_path)
    with pytest.raises(RuntimeError, match="local RDF state changed"):
        _execute(tmp_path, config_path, plan_path)


@pytest.mark.parametrize("separator", ["; ", " ; "])
def test_update_parts_split_on_both_separators(separator: str) -> None:
    query = (
        f"DELETE DATA {{ GRAPH <{BASE}br/> {{ <{BR_1}> <{CONTEXT}> <{AR_1}> . }} }}"
        f"{separator}INSERT DATA {{ GRAPH <{BASE}br/> {{ <{BR_1}> <{CONTEXT}> <{AR_2}> . }} }}"
    )

    assert fixer.triple_changes(query, fixer.CONTEXT_TRIPLE) == ({AR_2}, {AR_1})
