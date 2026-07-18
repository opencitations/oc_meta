# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import cast

import orjson
import pytest
import yaml
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler

from oc_meta.lib.agent_metadata import AgentMetadata, WorkMetadata
from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.patches import fix_dangling_ars as fixer
from test.test_utils import get_counter_handler

BASE = "https://w3id.org/oc/meta/"
BR_1 = f"{BASE}br/0601"
BR_2 = f"{BASE}br/0602"
AR_1 = f"{BASE}ar/0601"
AR_2 = f"{BASE}ar/0602"
AR_3 = f"{BASE}ar/0603"
AR_4 = f"{BASE}ar/0604"
RA_1 = f"{BASE}ra/0601"
ID_1 = f"{BASE}id/0601"
ID_2 = f"{BASE}id/0602"


def _write_entities(rdf_dir: Path, entities: list[dict[str, object]]) -> None:
    grouped: dict[str, list[dict[str, object]]] = {}
    for entity in entities:
        path = find_rdf_file(
            str(entity["@id"]), str(rdf_dir), 10000, 1000, zip_output=False
        )
        if path not in grouped:
            grouped[path] = []
        grouped[path].append(entity)
    for path, path_entities in grouped.items():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        graph_iri = str(path_entities[0]["@id"]).rsplit("/", 1)[0] + "/"
        Path(path).write_bytes(
            orjson.dumps([{"@id": graph_iri, "@graph": path_entities}])
        )


def _config(rdf_dir: Path) -> fixer.AuditConfig:
    return fixer.AuditConfig(str(rdf_dir), 10000, 1000, False)


def _write_config(tmp_path: Path, output_dir: Path) -> Path:
    config_path = tmp_path / "meta.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "base_output_dir": str(output_dir),
                "base_iri": BASE,
                "triplestore_url": "http://example.org/sparql",
                "provenance_triplestore_url": "http://example.org/provenance",
                "dir_split_number": 10000,
                "items_per_file": 1000,
                "zip_output_rdf": False,
                "supplier_prefix": "060",
                "rdf_files_only": True,
            }
        ),
        encoding="utf-8",
    )
    return config_path


def _agent(
    name: str,
    role: str,
    position: int,
    identifiers: tuple[tuple[str, str], ...] = (),
    family: str = "",
    given: str = "",
) -> AgentMetadata:
    return {
        "family": family,
        "given": given,
        "name": name,
        "orcid": next(
            (value for scheme, value in identifiers if scheme == "orcid"), None
        ),
        "identifiers": tuple(
            {"scheme": scheme, "value": value} for scheme, value in identifiers
        ),
        "position": position,
        "role": role,
    }


def _work(
    source: str,
    authors: list[AgentMetadata] | None = None,
    editors: list[AgentMetadata] | None = None,
    publisher: str = "",
) -> WorkMetadata:
    return {
        "identifier": "10.1000/example",
        "source": source,
        "author": [] if authors is None else authors,
        "editor": [] if editors is None else editors,
        "publisher": publisher,
        "publisher_identifiers": (),
    }


def _br_entity(
    uri: str,
    identifier_uri: str,
    role_uris: list[str],
    title: str = "Local title",
) -> dict[str, object]:
    return {
        "@id": uri,
        "@type": ["http://purl.org/spar/fabio/JournalArticle"],
        fixer.DCTERMS_TITLE: [{"@value": title}],
        fixer.IS_DOCUMENT_CONTEXT_FOR: [{"@id": role_uri} for role_uri in role_uris],
        fixer.HAS_IDENTIFIER: [{"@id": identifier_uri}],
    }


def _doi_entity(uri: str, doi: str) -> dict[str, object]:
    return {
        "@id": uri,
        "@type": ["http://purl.org/spar/datacite/Identifier"],
        fixer.USES_IDENTIFIER_SCHEME: [{"@id": "http://purl.org/spar/datacite/doi"}],
        fixer.HAS_LITERAL_VALUE: [{"@value": doi}],
    }


def _role_entity(
    uri: str, holder: str | None = RA_1, next_uri: str | None = None
) -> dict[str, object]:
    entity: dict[str, object] = {
        "@id": uri,
        "@type": ["http://purl.org/spar/pro/RoleInTime"],
        "http://purl.org/spar/pro/withRole": [
            {"@id": "http://purl.org/spar/pro/author"}
        ],
    }
    if holder is not None:
        entity["http://purl.org/spar/pro/isHeldBy"] = [{"@id": holder}]
    if next_uri is not None:
        entity[fixer.HAS_NEXT] = [{"@id": next_uri}]
    return entity


def _write_active_provenance(rdf_dir: Path, role_uri: str) -> Path:
    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    path = Path(fixer._provenance_path(locator.path(role_uri), False))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        orjson.dumps(
            [
                {
                    "@id": f"{role_uri}/prov/",
                    "@graph": [
                        {
                            "@id": f"{role_uri}/prov/se/1",
                            "@type": [
                                "http://www.w3.org/ns/prov#Entity",
                                "http://www.w3.org/ns/prov#Specialization",
                            ],
                            fixer.PROV_SPECIALIZATION_OF: [{"@id": role_uri}],
                            "http://www.w3.org/ns/prov#generatedAtTime": [
                                {
                                    "@value": "2026-07-13T10:00:00Z",
                                    "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                                }
                            ],
                            "http://www.w3.org/ns/prov#wasAttributedTo": [
                                {"@id": "https://example.org/agent"}
                            ],
                        }
                    ],
                }
            ]
        )
    )
    return path


def _set_counters(output_dir: Path, highest_ar: int, highest_id: int) -> None:
    counter_handler: FilesystemCounterHandler = get_counter_handler(
        str(output_dir / "info_dir"), "060"
    )
    counter_handler.set_counter(highest_ar, "ar", supplier_prefix="060")
    counter_handler.set_counter(1, "ra", supplier_prefix="060")
    counter_handler.set_counter(highest_id, "id", supplier_prefix="060")
    for role_id in range(1, highest_ar + 1):
        counter_handler.set_counter(
            1, "ar", "se", identifier=role_id, supplier_prefix="060"
        )
    counter_handler.flush()


class StubProvider:
    crossref_result: WorkMetadata | None = None
    datacite_result: WorkMetadata | None = None
    calls: list[tuple[str, str]] = []

    def __init__(self, mailto: str, cache: fixer.ApiCache, refresh_cache: bool) -> None:
        assert mailto == "test@example.org"
        assert isinstance(cache, fixer.ApiCache)
        assert refresh_cache is False

    def crossref(self, doi: str) -> WorkMetadata | None:
        self.calls.append(("crossref", doi))
        return self.crossref_result

    def datacite(self, doi: str) -> WorkMetadata | None:
        self.calls.append(("datacite", doi))
        return self.datacite_result

    def close(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _reset_provider() -> None:
    StubProvider.crossref_result = None
    StubProvider.datacite_result = None
    StubProvider.calls = []


def _analyze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config_path: Path
) -> tuple[dict[str, object], Path, Path]:
    monkeypatch.setattr(fixer, "AgentMetadataClient", StubProvider)
    report_path = tmp_path / "plan.json"
    csv_dir = tmp_path / "csv"
    report = fixer.analyze_dangling_ars(
        str(config_path),
        str(report_path),
        str(csv_dir),
        str(tmp_path / "api.sqlite"),
        "test@example.org",
        1,
        False,
    )
    return report, report_path, csv_dir


@pytest.mark.parametrize("workers", [1, 2])
def test_find_dangling_works_includes_all_local_roles(
    tmp_path: Path, workers: int
) -> None:
    rdf_dir = tmp_path / "rdf"
    malformed_role = {
        "@id": AR_1,
        "@type": ["http://purl.org/spar/pro/RoleInTime"],
        "http://purl.org/spar/pro/withRole": [
            {"@id": "http://purl.org/spar/pro/author"},
            {"@id": "http://purl.org/spar/pro/editor"},
        ],
    }
    _write_entities(
        rdf_dir,
        [_br_entity(BR_1, ID_1, [AR_1, AR_2]), malformed_role],
    )

    works, role_entities, missing, contexts = fixer.find_dangling_works(
        _config(rdf_dir), workers
    )

    assert works == {BR_1: fixer.WorkRecord(BR_1, (AR_1, AR_2), (ID_1,))}
    assert role_entities == {AR_1: malformed_role}
    assert missing == {BR_1: (AR_2,)}
    assert contexts == {AR_1: (BR_1,), AR_2: (BR_1,)}


def test_crossref_record_covers_empty_fields_and_writes_canonical_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    _write_entities(
        rdf_dir,
        [_br_entity(BR_1, ID_1, [AR_2]), _doi_entity(ID_1, "10.1000/example")],
    )
    StubProvider.crossref_result = _work(
        "crossref",
        [_agent("", "author", 0, (("orcid", "0000-0002-1825-0097"),), "Rossi", "Ada")],
    )

    report, _, csv_dir = _analyze(tmp_path, monkeypatch, config_path)
    with (csv_dir / "crossref" / "input.csv").open(
        newline="", encoding="utf-8"
    ) as stream:
        rows = list(csv.DictReader(stream))

    assert report["executable"] is True
    assert StubProvider.calls == [("crossref", "10.1000/example")]
    assert rows == [
        {
            "id": "omid:br/0601 doi:10.1000/example",
            "title": "Local title",
            "author": "Rossi, Ada [orcid:0000-0002-1825-0097]",
            "issue": "",
            "volume": "",
            "venue": "",
            "page": "",
            "pub_date": "",
            "type": "journal article",
            "publisher": "",
            "editor": "",
        }
    ]
    assert list(csv.DictReader((csv_dir / "datacite" / "input.csv").open())) == []


def test_datacite_is_the_only_fallback_and_serializes_all_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    _write_entities(
        rdf_dir,
        [_br_entity(BR_1, ID_1, [AR_2]), _doi_entity(ID_1, "10.1000/example")],
    )
    datacite = _work(
        "datacite",
        [_agent("Ada Rossi", "author", 0)],
        [_agent("Luca Bianchi", "editor", 0)],
        "Data Press",
    )
    datacite["publisher_identifiers"] = ({"scheme": "ror", "value": "03yrm5c26"},)
    StubProvider.datacite_result = datacite

    report, _, csv_dir = _analyze(tmp_path, monkeypatch, config_path)
    rows = list(
        csv.DictReader(
            (csv_dir / "datacite" / "input.csv").open(newline="", encoding="utf-8")
        )
    )

    assert report["executable"] is True
    assert StubProvider.calls == [
        ("crossref", "10.1000/example"),
        ("datacite", "10.1000/example"),
    ]
    assert rows[0]["author"] == "Ada Rossi"
    assert rows[0]["editor"] == "Luca Bianchi"
    assert rows[0]["publisher"] == "Data Press [ror:03yrm5c26]"


def test_provider_failure_writes_report_without_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    _write_entities(
        rdf_dir,
        [_br_entity(BR_1, ID_1, [AR_2]), _doi_entity(ID_1, "10.1000/example")],
    )

    report, report_path, csv_dir = _analyze(tmp_path, monkeypatch, config_path)

    assert report_path.exists()
    assert report["executable"] is False
    assert report["csv_files"] == {}
    assert cast(dict[str, object], report["summary"])["blocker_counts"] == {
        "provider_unavailable": 1
    }
    assert not csv_dir.exists()


@pytest.mark.parametrize("external_link", [False, True])
def test_external_contexts_and_has_next_links_block_the_whole_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    external_link: bool,
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    external_br = f"{BASE}br/06099"
    external_ar = f"{BASE}ar/06099"
    role = _role_entity(AR_1, next_uri=external_ar if external_link else None)
    entities = [
        _br_entity(BR_1, ID_1, [AR_1, AR_2]),
        _doi_entity(ID_1, "10.1000/example"),
        role,
    ]
    if external_link:
        entities.append(_role_entity(external_ar))
    else:
        entities.append(
            {
                "@id": external_br,
                fixer.IS_DOCUMENT_CONTEXT_FOR: [{"@id": AR_1}],
            }
        )
    _write_entities(rdf_dir, entities)
    StubProvider.crossref_result = _work("crossref")

    report, _, csv_dir = _analyze(tmp_path, monkeypatch, config_path)

    expected = "external_has_next" if external_link else "shared_agent_role"
    assert report["executable"] is False
    assert cast(dict[str, object], report["summary"])["blocker_counts"] == {expected: 1}
    assert StubProvider.calls == []
    assert not csv_dir.exists()


def test_execute_removes_every_role_and_preserves_agents_and_identifiers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    malformed_role = _role_entity(AR_1, holder=None, next_uri=AR_2)
    malformed_role["http://purl.org/spar/pro/withRole"] = [
        {"@id": "http://purl.org/spar/pro/author"},
        {"@id": "http://purl.org/spar/pro/editor"},
    ]
    ra_entity = {
        "@id": RA_1,
        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
        "http://xmlns.com/foaf/0.1/name": [{"@value": "Preserved agent"}],
        fixer.HAS_IDENTIFIER: [{"@id": ID_2}],
    }
    agent_id_entity = {
        "@id": ID_2,
        "@type": ["http://purl.org/spar/datacite/Identifier"],
        fixer.USES_IDENTIFIER_SCHEME: [{"@id": "http://purl.org/spar/datacite/orcid"}],
        fixer.HAS_LITERAL_VALUE: [{"@value": "0000-0002-1825-0097"}],
    }
    _write_entities(
        rdf_dir,
        [
            _br_entity(BR_1, ID_1, [AR_1, AR_2]),
            malformed_role,
            ra_entity,
            _doi_entity(ID_1, "10.1000/example"),
            agent_id_entity,
        ],
    )
    provenance_path = _write_active_provenance(rdf_dir, AR_2)
    _set_counters(output_dir, 2, 2)
    StubProvider.crossref_result = _work("crossref")
    report, report_path, _ = _analyze(tmp_path, monkeypatch, config_path)
    operation = cast(list[dict[str, object]], report["operations"])[0]

    execution = fixer.execute_plan(
        str(config_path),
        str(report_path),
        "https://example.org/agent",
        str(tmp_path / "progress.json"),
        str(tmp_path / "execution.json"),
        1,
    )

    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    br_entity = fixer._load_entities(locator.path(BR_1))[BR_1]
    remaining_roles = fixer.load_available_entities({AR_1, AR_2}, locator, 1)
    ra_after = fixer.load_available_entities({RA_1}, locator, 1)[RA_1]
    id_after = fixer.load_available_entities({ID_2}, locator, 1)[ID_2]
    snapshots = fixer._load_entities(str(provenance_path))
    missing_snapshots = [
        snapshot
        for snapshot in snapshots.values()
        if fixer._ids(snapshot, fixer.PROV_SPECIALIZATION_OF) == [AR_2]
    ]

    assert cast(dict[str, object], operation["actions"]) == {
        "remove_role_references": [AR_1, AR_2],
        "delete_existing_ars": [AR_1],
        "invalidate_missing_ars": [AR_2],
    }
    assert execution["complete"] is True
    assert fixer._ids(br_entity, fixer.IS_DOCUMENT_CONTEXT_FOR) == []
    assert remaining_roles == {}
    assert ra_after == ra_entity
    assert id_after == agent_id_entity
    assert len(missing_snapshots) == 2
    assert (
        len(fixer._literals(missing_snapshots[-1], fixer.PROV_INVALIDATED_AT_TIME)) == 1
    )
    assert Path(cast(str, execution["reindex_sentinel"])).exists()


@pytest.mark.parametrize("changed", ["version", "operation", "config", "csv"])
def test_execution_rejects_changed_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed: str,
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    _write_entities(
        rdf_dir,
        [_br_entity(BR_1, ID_1, [AR_2]), _doi_entity(ID_1, "10.1000/example")],
    )
    StubProvider.crossref_result = _work("crossref")
    _, report_path, csv_dir = _analyze(tmp_path, monkeypatch, config_path)
    expected = ""
    if changed == "version":
        plan = orjson.loads(report_path.read_bytes())
        plan["version"] = 1
        report_path.write_bytes(orjson.dumps(plan))
        expected = "Unsupported correction plan version"
    elif changed == "operation":
        plan = orjson.loads(report_path.read_bytes())
        plan["operations"][0]["work"]["title"] = "Changed"
        report_path.write_bytes(orjson.dumps(plan))
        expected = "modified operation"
    elif changed == "config":
        config_path.write_text(
            config_path.read_text(encoding="utf-8") + "\nnormalize_titles: false\n",
            encoding="utf-8",
        )
        expected = "configuration changed"
    else:
        with (csv_dir / "crossref" / "input.csv").open("a", encoding="utf-8") as stream:
            stream.write("changed\n")
        expected = "CSV changed"

    with pytest.raises(ValueError, match=expected):
        fixer.execute_plan(
            str(config_path),
            str(report_path),
            "https://example.org/agent",
            str(tmp_path / "progress.json"),
            str(tmp_path / "execution.json"),
            1,
        )


def test_execution_resumes_the_same_plan_after_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
    config_path = _write_config(tmp_path, output_dir)
    _write_entities(
        rdf_dir,
        [
            _br_entity(BR_1, ID_1, [AR_1, AR_2], "First"),
            _br_entity(BR_2, ID_2, [AR_3, AR_4], "Second"),
            _role_entity(AR_1),
            _role_entity(AR_3),
            _doi_entity(ID_1, "10.1000/first"),
            _doi_entity(ID_2, "10.1000/second"),
        ],
    )
    _set_counters(output_dir, 4, 2)
    StubProvider.crossref_result = _work("crossref")
    _, report_path, _ = _analyze(tmp_path, monkeypatch, config_path)
    progress_path = tmp_path / "progress.json"
    original_apply = fixer._apply_operation
    applications = 0

    def interrupt_after_first(
        editor: fixer.MetaEditor,
        operation: dict[str, object],
        locator: fixer.EntityFileLocator,
    ) -> None:
        nonlocal applications
        original_apply(editor, operation, locator)
        applications += 1
        if applications == 1:
            fixer._stop_requested = True

    monkeypatch.setattr(fixer, "_apply_operation", interrupt_after_first)
    first = fixer.execute_plan(
        str(config_path),
        str(report_path),
        "https://example.org/agent",
        str(progress_path),
        str(tmp_path / "execution.json"),
        1,
    )
    monkeypatch.setattr(fixer, "_apply_operation", original_apply)
    second = fixer.execute_plan(
        str(config_path),
        str(report_path),
        "https://example.org/agent",
        str(progress_path),
        str(tmp_path / "execution.json"),
        1,
    )

    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    br_1 = fixer._load_entities(locator.path(BR_1))[BR_1]
    br_2 = fixer._load_entities(locator.path(BR_2))[BR_2]
    assert first["complete"] is False
    assert progress_path.exists() is False
    assert second["complete"] is True
    assert fixer._ids(br_1, fixer.IS_DOCUMENT_CONTEXT_FOR) == []
    assert fixer._ids(br_2, fixer.IS_DOCUMENT_CONTEXT_FOR) == []


def test_csv_serialization_keeps_all_2880_authors() -> None:
    authors = [
        _agent(f"Author {position}", "author", position) for position in range(2880)
    ]

    serialized = fixer.serialize_agents(authors)

    assert serialized.split("; ") == [f"Author {position}" for position in range(2880)]
