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
from oc_meta.lib.agent_matching import PersonName
from oc_meta.lib.agent_metadata import AgentMetadata, WorkMetadata
from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.patches import fix_dangling_ars as fixer
from test.test_utils import get_counter_handler

BASE = "https://w3id.org/oc/meta/"
BR = f"{BASE}br/0601"
AR_1 = f"{BASE}ar/0601"
AR_2 = f"{BASE}ar/0602"
AR_3 = f"{BASE}ar/0603"
RA_1 = f"{BASE}ra/0601"
RA_2 = f"{BASE}ra/0602"
ID_1 = f"{BASE}id/0601"


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


def _agent(
    name: str,
    role: str,
    position: int,
    identifiers: tuple[tuple[str, str], ...] = (),
) -> AgentMetadata:
    return {
        "family": "",
        "given": "",
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
    authors: list[AgentMetadata],
    editors: list[AgentMetadata],
    publisher: str = "",
) -> WorkMetadata:
    return {
        "identifier": "10.1000/example",
        "source": source,
        "author": authors,
        "editor": editors,
        "publisher": publisher,
        "publisher_identifiers": (),
    }


@pytest.mark.parametrize("workers", [1, 2])
def test_find_dangling_works_ignores_existing_malformed_local_roles(
    tmp_path: Path, workers: int
) -> None:
    rdf_dir = tmp_path / "rdf"
    br_2 = f"{BASE}br/0602"
    _write_entities(
        rdf_dir,
        [
            {
                "@id": BR,
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": AR_1},
                    {"@id": AR_2},
                ],
            },
            {
                "@id": br_2,
                "http://purl.org/spar/pro/isDocumentContextFor": [{"@id": AR_3}],
            },
            {
                "@id": AR_1,
                "http://purl.org/spar/pro/withRole": [
                    {"@id": "http://purl.org/spar/pro/author"}
                ],
                "http://purl.org/spar/pro/isHeldBy": [{"@id": RA_1}],
            },
            {"@id": AR_3},
        ],
    )

    works, role_entities, missing, contexts = fixer.find_dangling_works(
        _config(rdf_dir), workers
    )

    assert works == {
        BR: fixer.WorkRecord(BR, (AR_1, AR_2), ()),
    }
    assert role_entities == {AR_1: role_entities[AR_1]}
    assert missing == {BR: (AR_2,)}
    assert contexts == {AR_1: (BR,), AR_2: (BR,)}


@pytest.mark.parametrize("workers", [1, 2])
def test_scan_identity_index_returns_exact_identifier_holders(
    tmp_path: Path, workers: int
) -> None:
    rdf_dir = tmp_path / "rdf"
    orcid = "0000-0002-1825-0097"
    _write_entities(
        rdf_dir,
        [
            {
                "@id": ID_1,
                fixer.USES_IDENTIFIER_SCHEME: [
                    {"@id": "http://purl.org/spar/datacite/orcid"}
                ],
                fixer.HAS_LITERAL_VALUE: [{"@value": orcid}],
            },
            {
                "@id": RA_1,
                fixer.HAS_IDENTIFIER: [{"@id": ID_1}],
            },
        ],
    )

    assert fixer.scan_identity_index(_config(rdf_dir), {("orcid", orcid)}, workers) == {
        ("orcid", orcid): fixer.IdentityEntry((ID_1,), (RA_1,)),
    }


def test_structural_anomalies_detects_multiple_predecessors() -> None:
    work = fixer.WorkRecord(BR, (AR_1, AR_2, AR_3), ())
    roles = {
        AR_1: fixer.RoleRecord(
            AR_1,
            ("http://purl.org/spar/pro/author",),
            (RA_1,),
            (AR_3,),
        ),
        AR_2: fixer.RoleRecord(
            AR_2,
            ("http://purl.org/spar/pro/author",),
            (RA_2,),
            (AR_3,),
        ),
        AR_3: fixer.RoleRecord(
            AR_3,
            ("http://purl.org/spar/pro/author",),
            (RA_1,),
            (),
        ),
    }
    contexts = {AR_1: (BR,), AR_2: (BR,), AR_3: (BR,)}

    assert fixer._structural_anomalies(work, roles, contexts) == [
        {
            "type": "multiple_predecessors",
            "ar": AR_3,
            "predecessor_count": 2,
        }
    ]


def test_select_provider_targets_uses_first_nonempty_value_per_role() -> None:
    crossref_author = _agent("Ada Rossi", "author", 0)
    datacite_editor = _agent("Luca Bianchi", "editor", 0)
    works = [
        _work("crossref", [crossref_author], []),
        _work("datacite", [], [datacite_editor], "Data Press"),
        _work("openalex", [_agent("Ignored", "author", 0)], []),
    ]

    targets, sources = fixer.select_provider_targets(works)

    assert targets == {
        "author": [crossref_author],
        "editor": [datacite_editor],
        "publisher": [
            {
                "family": "",
                "given": "",
                "name": "Data Press",
                "orcid": None,
                "identifiers": (),
                "position": 0,
                "role": "publisher",
            }
        ],
    }
    assert sources == {
        "author": "crossref",
        "editor": "datacite",
        "publisher": "datacite",
    }


def test_select_provider_targets_skips_empty_values_and_preserves_when_all_empty() -> (
    None
):
    datacite_editor = _agent("Luca Bianchi", "editor", 0)
    targets, sources = fixer.select_provider_targets(
        [
            _work("openalex", [_agent("Ignored", "author", 0)], []),
            _work("datacite", [], [datacite_editor]),
            _work("crossref", [], []),
        ]
    )

    assert targets == {
        "author": [_agent("Ignored", "author", 0)],
        "editor": [datacite_editor],
        "publisher": [],
    }
    assert sources == {
        "author": "openalex",
        "editor": "datacite",
        "publisher": None,
    }

    empty_targets, empty_sources = fixer.select_provider_targets(
        [
            _work("crossref", [], []),
            _work("datacite", [], []),
            _work("openalex", [], []),
        ]
    )

    assert empty_targets == {"author": [], "editor": [], "publisher": []}
    assert empty_sources == {"author": None, "editor": None, "publisher": None}


def test_absent_provider_role_preserves_local_chain() -> None:
    roles = [
        fixer.RoleRecord(
            AR_1,
            ("http://purl.org/spar/pro/editor",),
            (RA_1,),
            (AR_3,),
        ),
        fixer.RoleRecord(
            AR_3,
            ("http://purl.org/spar/pro/editor",),
            (RA_2,),
            (),
        ),
    ]
    agents = {
        RA_1: fixer.AgentRecord(RA_1, PersonName(name="Ada Rossi"), ()),
        RA_2: fixer.AgentRecord(RA_2, PersonName(name="Luca Bianchi"), ()),
    }

    plan = fixer._preserve_role("editor", roles, agents)

    assert plan == {
        "role": "editor",
        "source": None,
        "targets": [
            {
                "position": 0,
                "agent": {
                    "family": "",
                    "given": "",
                    "name": "Ada Rossi",
                    "identifiers": [],
                },
                "source": None,
                "ar": AR_1,
                "old_ra": RA_1,
                "ra": RA_1,
                "ra_action": "reuse",
                "resolution": "local",
                "identifier_resolutions": [],
                "old_next": AR_3,
            },
            {
                "position": 1,
                "agent": {
                    "family": "",
                    "given": "",
                    "name": "Luca Bianchi",
                    "identifiers": [],
                },
                "source": None,
                "ar": AR_3,
                "old_ra": RA_2,
                "ra": RA_2,
                "ra_action": "reuse",
                "resolution": "local",
                "identifier_resolutions": [],
                "old_next": None,
            },
        ],
        "delete_ars": [],
    }
    assert fixer._review_changes([], [plan]) == []


def test_reconcile_role_preserves_name_match_and_reuses_remaining_ar() -> None:
    roles = [
        fixer.RoleRecord(
            AR_1,
            ("http://purl.org/spar/pro/author",),
            (RA_1,),
            (),
        ),
        fixer.RoleRecord(
            AR_3,
            ("http://purl.org/spar/pro/author",),
            (RA_2,),
            (),
        ),
    ]
    agents = {
        RA_1: fixer.AgentRecord(RA_1, PersonName(name="Ada Rossi"), ()),
        RA_2: fixer.AgentRecord(RA_2, PersonName(name="Old Agent"), ()),
    }
    external = [
        _agent("Ada Rossi", "author", 0),
        _agent("Bob Smith", "author", 1),
        _agent("Carol Jones", "author", 2),
    ]

    role_plan, error = fixer._reconcile_role(
        "author", roles, external, agents, {}, "crossref"
    )

    assert error is None
    assert role_plan == {
        "role": "author",
        "source": "crossref",
        "targets": [
            {
                "position": 0,
                "agent": {
                    "family": "",
                    "given": "",
                    "name": "Ada Rossi",
                    "identifiers": [],
                },
                "source": "crossref",
                "ar": AR_1,
                "old_ra": RA_1,
                "ra": RA_1,
                "ra_action": "reuse",
                "resolution": "name",
                "identifier_resolutions": [],
                "old_next": None,
            },
            {
                "position": 1,
                "agent": {
                    "family": "",
                    "given": "",
                    "name": "Bob Smith",
                    "identifiers": [],
                },
                "source": "crossref",
                "ar": AR_3,
                "old_ra": RA_2,
                "ra": None,
                "ra_action": "create",
                "resolution": "new",
                "identifier_resolutions": [],
                "old_next": None,
            },
            {
                "position": 2,
                "agent": {
                    "family": "",
                    "given": "",
                    "name": "Carol Jones",
                    "identifiers": [],
                },
                "source": "crossref",
                "ar": None,
                "old_ra": None,
                "ra": None,
                "ra_action": "create",
                "resolution": "new",
                "identifier_resolutions": [],
                "old_next": None,
            },
        ],
        "delete_ars": [],
    }
    assert [change["action"] for change in fixer._review_changes([], [role_plan])] == [
        "create_responsible_agent",
        "reassign_agent_role",
        "create_responsible_agent",
        "create_agent_role",
        "update_next_link",
        "update_next_link",
    ]


def test_reconcile_role_uses_exact_identifier_and_blocks_ambiguous_identity() -> None:
    orcid = "0000-0002-8420-0696"
    external = [_agent("Ada Rossi", "author", 0, (("orcid", orcid),))]
    role = fixer.RoleRecord(
        AR_1,
        ("http://purl.org/spar/pro/author",),
        (RA_1,),
        (),
    )
    agents = {RA_1: fixer.AgentRecord(RA_1, PersonName(name="Wrong"), ())}
    key = ("orcid", orcid)

    plan, error = fixer._reconcile_role(
        "author",
        [role],
        external,
        agents,
        {key: fixer.IdentityEntry((ID_1,), (RA_2,))},
        "crossref",
    )
    _, ambiguous_error = fixer._reconcile_role(
        "author",
        [role],
        external,
        agents,
        {key: fixer.IdentityEntry((ID_1,), (RA_1, RA_2))},
        "crossref",
    )

    assert error is None
    assert plan["targets"] == [
        {
            "position": 0,
            "agent": {
                "family": "",
                "given": "",
                "name": "Ada Rossi",
                "identifiers": [{"scheme": "orcid", "value": orcid}],
            },
            "source": "crossref",
            "ar": AR_1,
            "old_ra": RA_1,
            "ra": RA_2,
            "ra_action": "reuse",
            "resolution": "identifier",
            "identifier_resolutions": [
                {
                    "scheme": "orcid",
                    "value": orcid,
                    "identifier_uris": [ID_1],
                    "ra_uris": [RA_2],
                }
            ],
            "old_next": None,
        }
    ]
    assert ambiguous_error == {
        "type": "ambiguous_identifier",
        "role": "author",
        "position": 0,
        "aligned_ar": AR_1,
        "aligned_holder": RA_1,
        "candidate_ras": [RA_1, RA_2],
        "identifier_resolutions": [
            {
                "scheme": "orcid",
                "value": orcid,
                "identifier_uris": [ID_1],
                "ra_uris": [RA_1, RA_2],
            }
        ],
    }


@pytest.mark.parametrize("count", [2, 3, 5])
def test_reconcile_role_preserves_repeated_homonyms_by_position(count: int) -> None:
    ar_uris = [f"{BASE}ar/07{index:02d}" for index in range(count)]
    ra_uris = [f"{BASE}ra/07{index:02d}" for index in range(count)]
    roles = [
        fixer.RoleRecord(
            ar_uri,
            ("http://purl.org/spar/pro/author",),
            (ra_uri,),
            (ar_uris[index + 1],) if index + 1 < count else (),
        )
        for index, (ar_uri, ra_uri) in enumerate(zip(ar_uris, ra_uris))
    ]
    agents = {
        ra_uri: fixer.AgentRecord(ra_uri, PersonName(name="Wei Wang"), ())
        for ra_uri in ra_uris
    }
    external = [_agent("Wei Wang", "author", index) for index in range(count)]

    plan, blocker = fixer._reconcile_role(
        "author", roles, external, agents, {}, "crossref"
    )
    targets = cast(list[dict[str, object]], plan["targets"])

    assert blocker is None
    assert [target["ar"] for target in targets] == ar_uris
    assert [target["ra"] for target in targets] == ra_uris
    assert [target["ra_action"] for target in targets] == ["reuse"] * count
    assert [target["resolution"] for target in targets] == ["position_name"] * count
    assert fixer._review_changes([], [plan]) == []


def test_ambiguous_orcid_preserves_compatible_contextual_holders() -> None:
    orcid = "0000-0002-8420-0696"
    key = ("orcid", orcid)
    roles = [
        fixer.RoleRecord(
            AR_1,
            ("http://purl.org/spar/pro/author",),
            (RA_1,),
            (AR_3,),
        ),
        fixer.RoleRecord(
            AR_3,
            ("http://purl.org/spar/pro/author",),
            (RA_2,),
            (),
        ),
    ]
    agents = {
        RA_1: fixer.AgentRecord(RA_1, PersonName(name="Wei Wang"), ()),
        RA_2: fixer.AgentRecord(RA_2, PersonName(name="Wei Wang"), ()),
    }
    external = [
        _agent("Wei Wang", "author", position, (("orcid", orcid),))
        for position in range(2)
    ]

    plan, blocker = fixer._reconcile_role(
        "author",
        roles,
        external,
        agents,
        {key: fixer.IdentityEntry((ID_1,), (RA_1, RA_2))},
        "crossref",
    )
    targets = cast(list[dict[str, object]], plan["targets"])

    assert blocker is None
    assert [target["ra"] for target in targets] == [RA_1, RA_2]
    assert [target["resolution"] for target in targets] == [
        "ambiguous_identifier_local",
        "ambiguous_identifier_local",
    ]


@pytest.mark.parametrize(
    ("resolution", "planned_ras", "current_ras"),
    [
        ("new", (), (RA_1,)),
        ("name", (), (RA_1,)),
        ("position_name", (), (RA_1,)),
        ("identifier", (RA_1,), (RA_2,)),
        ("ambiguous_identifier_local", (RA_1, RA_2), (RA_1,)),
    ],
)
def test_execution_rejects_changed_global_identity_resolution(
    resolution: str,
    planned_ras: tuple[str, ...],
    current_ras: tuple[str, ...],
) -> None:
    orcid = "0000-0002-8420-0696"
    repair = {
        "work": {"br": BR},
        "execution": {
            "role_plans": [
                {
                    "targets": [
                        {
                            "agent": {
                                "family": "",
                                "given": "",
                                "name": "Ada Rossi",
                                "identifiers": [{"scheme": "orcid", "value": orcid}],
                            },
                            "resolution": resolution,
                            "identifier_resolutions": [
                                {
                                    "scheme": "orcid",
                                    "value": orcid,
                                    "identifier_uris": [ID_1],
                                    "ra_uris": list(planned_ras),
                                }
                            ],
                        }
                    ]
                }
            ]
        },
    }

    with pytest.raises(RuntimeError, match="identifier resolution changed"):
        fixer._validate_identity_index(
            repair,
            {("orcid", orcid): fixer.IdentityEntry((ID_1,), current_ras)},
        )


@pytest.mark.parametrize("workers", [1, 2])
def test_load_provenance_statuses_distinguishes_active_invalidated_and_missing(
    tmp_path: Path, workers: int
) -> None:
    rdf_dir = tmp_path / "rdf"
    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    provenance_path = Path(fixer._provenance_path(locator.path(AR_1), False))
    provenance_path.parent.mkdir(parents=True)
    provenance_path.write_bytes(
        orjson.dumps(
            [
                {
                    "@id": f"{AR_2}/prov/",
                    "@graph": [
                        {
                            "@id": f"{AR_1}/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": AR_1}
                            ],
                        },
                        {
                            "@id": f"{AR_2}/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": AR_2}
                            ],
                            "http://www.w3.org/ns/prov#invalidatedAtTime": [
                                {"@value": "2026-07-14T10:00:00Z"}
                            ],
                        },
                    ],
                }
            ]
        )
    )

    statuses = fixer.load_provenance_statuses({AR_1, AR_2, AR_3}, locator, workers)

    assert statuses == {
        AR_1: "latest_snapshot_active",
        AR_2: "latest_snapshot_invalidated",
        AR_3: "no_snapshot",
    }


def test_preconditions_detect_role_context_that_becomes_shared(tmp_path: Path) -> None:
    rdf_dir = tmp_path / "rdf"
    br_2 = f"{BASE}br/0602"
    entities = [
        {
            "@id": BR,
            "http://purl.org/spar/pro/isDocumentContextFor": [
                {"@id": AR_1},
                {"@id": AR_2},
            ],
        },
        {
            "@id": AR_1,
            "http://purl.org/spar/pro/withRole": [
                {"@id": "http://purl.org/spar/pro/author"}
            ],
            "http://purl.org/spar/pro/isHeldBy": [{"@id": RA_1}],
        },
        {
            "@id": RA_1,
            "http://xmlns.com/foaf/0.1/name": [{"@value": "Ada Rossi"}],
        },
    ]
    _write_entities(rdf_dir, entities)
    repair: dict[str, object] = {"work": {"br": BR}}
    config = _config(rdf_dir)

    before = fixer._capture_preconditions(repair, config, 1)
    _write_entities(
        rdf_dir,
        [
            *entities,
            {
                "@id": br_2,
                "http://purl.org/spar/pro/isDocumentContextFor": [{"@id": AR_1}],
            },
        ],
    )
    after = fixer._capture_preconditions(repair, config, 1)

    assert cast(dict[str, list[str]], before["contexts"]) == {
        AR_1: [BR],
        AR_2: [BR],
    }
    assert cast(dict[str, list[str]], after["contexts"]) == {
        AR_1: [BR, br_2],
        AR_2: [BR],
    }
    assert before != after


def test_review_approval_is_atomic_per_br_and_detects_modified_plan(
    tmp_path: Path,
) -> None:
    repair = fixer._finalize_repair(
        {
            "work": {
                "br": BR,
                "title": "Example",
                "identifiers": {"doi": "10.1000/example"},
            },
            "review": {
                "status": "ready",
                "problem": {"dangling_ar_references": []},
                "provider_records_found": ["crossref"],
                "selected_provider_by_role": {
                    "author": "crossref",
                    "editor": None,
                    "publisher": None,
                },
                "changes": [],
                "warnings": [],
                "blockers": [],
            },
            "execution": {
                "preconditions": {
                    "entities": {},
                    "dangling_ar_references": [AR_2],
                    "provenance": {AR_2: "no_snapshot"},
                    "contexts": {AR_2: [BR]},
                },
                "role_plans": [],
            },
        }
    )
    review_path = tmp_path / "review.csv"
    fixer.write_review_file(str(review_path), [repair])
    rows = list(csv.DictReader(review_path.open(encoding="utf-8")))
    assert list(rows[0]) == ["group_id", "br", "doi", "status", "decision"]
    rows[0]["decision"] = "approve"
    with review_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fixer.REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    approved = fixer.read_review_decisions(str(review_path), [repair])
    cast(dict[str, object], repair["review"])["warnings"] = [{"type": "changed"}]

    assert approved == [repair]
    with pytest.raises(ValueError, match="modified repair group"):
        fixer.read_review_decisions(str(review_path), [repair])


def test_blocked_review_group_cannot_be_approved(tmp_path: Path) -> None:
    repair = fixer._finalize_repair(
        {
            "work": {"br": BR, "title": None, "identifiers": {}},
            "review": {
                "status": "blocked",
                "problem": {"dangling_ar_references": []},
                "provider_records_found": [],
                "selected_provider_by_role": {
                    "author": None,
                    "editor": None,
                    "publisher": None,
                },
                "changes": [],
                "warnings": [],
                "blockers": [{"type": "invalid_holder", "ar": AR_1}],
            },
            "execution": {"preconditions": {}, "role_plans": []},
        }
    )
    review_path = tmp_path / "review.csv"
    fixer.write_review_file(str(review_path), [repair])
    rows = list(csv.DictReader(review_path.open(encoding="utf-8")))
    rows[0]["decision"] = "approve"
    with review_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fixer.REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(ValueError, match="cannot be approved"):
        fixer.read_review_decisions(str(review_path), [repair])


class CrossrefProvider:
    def __init__(
        self,
        mailto: str,
        cache: fixer.ApiCache,
        refresh_cache: bool,
        openalex_api_key: str,
    ) -> None:
        assert mailto == "test@example.org"
        assert isinstance(cache, fixer.ApiCache)
        assert refresh_cache is False
        assert openalex_api_key == ""

    def all_work_sources(self, doi: str, openalex_id: str = "") -> list[WorkMetadata]:
        assert doi == "10.1000/example"
        assert openalex_id == ""
        return [
            _work(
                "crossref",
                [
                    _agent("Ada Rossi", "author", 0),
                    _agent(
                        "Bob Smith",
                        "author",
                        1,
                        (("orcid", "0000-0002-1825-0097"),),
                    ),
                ],
                [],
            )
        ]

    def close(self) -> None:
        return None


def _approve_review(path: Path) -> None:
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    assert len(rows) == 1
    rows[0]["decision"] = "approve"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fixer.REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def test_execute_plan_rebuilds_chain_and_invalidates_missing_ar_provenance(
    tmp_path: Path, monkeypatch
) -> None:
    output_dir = tmp_path / "output"
    rdf_dir = output_dir / "rdf"
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
    _write_entities(
        rdf_dir,
        [
            {
                "@id": BR,
                "@type": ["http://purl.org/spar/fabio/Expression"],
                "http://purl.org/dc/terms/title": [{"@value": "Untouched title"}],
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": AR_1},
                    {"@id": AR_2},
                ],
                "http://purl.org/spar/datacite/hasIdentifier": [{"@id": ID_1}],
            },
            {
                "@id": AR_1,
                "@type": ["http://purl.org/spar/pro/RoleInTime"],
                "http://purl.org/spar/pro/withRole": [
                    {"@id": "http://purl.org/spar/pro/author"}
                ],
                "http://purl.org/spar/pro/isHeldBy": [{"@id": RA_1}],
            },
            {
                "@id": RA_1,
                "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                "http://xmlns.com/foaf/0.1/name": [{"@value": "Ada Rossi"}],
            },
            {
                "@id": ID_1,
                "@type": ["http://purl.org/spar/datacite/Identifier"],
                "http://purl.org/spar/datacite/usesIdentifierScheme": [
                    {"@id": "http://purl.org/spar/datacite/doi"}
                ],
                "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                    {"@value": "10.1000/example"}
                ],
            },
        ],
    )
    locator = fixer.EntityFileLocator(str(rdf_dir), 10000, 1000, False)
    provenance_path = Path(fixer._provenance_path(locator.path(AR_2), False))
    provenance_path.parent.mkdir(parents=True)
    provenance_path.write_bytes(
        orjson.dumps(
            [
                {
                    "@id": f"{AR_2}/prov/",
                    "@graph": [
                        {
                            "@id": f"{AR_2}/prov/se/1",
                            "@type": [
                                "http://www.w3.org/ns/prov#Entity",
                                "http://www.w3.org/ns/prov#Specialization",
                            ],
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": AR_2}
                            ],
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
    counter_handler: FilesystemCounterHandler = get_counter_handler(
        str(output_dir / "info_dir"), "060"
    )
    counter_handler.set_counter(2, "ar", supplier_prefix="060")
    counter_handler.set_counter(1, "ra", supplier_prefix="060")
    counter_handler.set_counter(1, "id", supplier_prefix="060")
    counter_handler.set_counter(1, "ar", "se", identifier=2, supplier_prefix="060")
    counter_handler.flush()
    monkeypatch.setattr(fixer, "AgentMetadataClient", CrossrefProvider)

    report_path = tmp_path / "plan.json"
    review_path = tmp_path / "review.csv"
    report = fixer.analyze_dangling_ars(
        str(config_path),
        str(report_path),
        str(review_path),
        str(tmp_path / "api.sqlite"),
        "test@example.org",
        1,
        False,
        "",
    )
    assert set(report) == {
        "complete",
        "generated_at",
        "config",
        "config_sha256",
        "rdf_dir",
        "api_cache",
        "review_file",
        "summary",
        "repairs",
    }
    repair = cast(list[dict[str, object]], report["repairs"])[0]
    review = cast(dict[str, object], repair["review"])
    planned_execution = cast(dict[str, object], repair["execution"])
    assert set(repair) == {"group_id", "work", "review", "execution"}
    assert cast(dict[str, object], repair["work"]) == {
        "br": BR,
        "title": "Untouched title",
        "identifiers": {"doi": "10.1000/example"},
    }
    assert [
        change["action"] for change in cast(list[dict[str, object]], review["changes"])
    ] == [
        "remove_dangling_reference",
        "create_responsible_agent",
        "create_agent_role",
        "update_next_link",
    ]
    assert list(review) == [
        "status",
        "problem",
        "provider_records_found",
        "selected_provider_by_role",
        "changes",
        "warnings",
        "blockers",
    ]
    assert set(planned_execution) == {"preconditions", "role_plans"}
    assert "entities" not in review
    _approve_review(review_path)
    execution = fixer.execute_plan(
        str(config_path),
        str(report_path),
        str(review_path),
        "https://example.org/agent",
        str(tmp_path / "progress.json"),
        str(tmp_path / "execution.json"),
        1,
    )
    assert set(execution) == {
        "plan",
        "plan_sha256",
        "review_file",
        "review_sha256",
        "generated_at",
        "complete",
        "approved_groups",
        "completed_groups",
        "reindex_sentinel",
    }

    br_entity = fixer._load_entities(locator.path(BR))[BR]
    role_uris = fixer._ids(br_entity, fixer.IS_DOCUMENT_CONTEXT_FOR)
    new_ar = next(uri for uri in role_uris if uri != AR_1)
    role_entities = fixer.load_available_entities(set(role_uris), locator, 1)
    new_ra = fixer._ids(role_entities[new_ar], fixer.IS_HELD_BY)[0]
    ra_entity = fixer.load_available_entities({new_ra}, locator, 1)[new_ra]
    new_id = fixer._ids(ra_entity, fixer.HAS_IDENTIFIER)[0]
    id_entity = fixer.load_available_entities({new_id}, locator, 1)[new_id]
    snapshots = fixer._load_entities(str(provenance_path))
    missing_snapshots = [
        snapshot
        for snapshot in snapshots.values()
        if fixer._ids(snapshot, fixer.PROV_SPECIALIZATION_OF) == [AR_2]
    ]

    assert cast(dict[str, object], report["summary"]) == {
        "affected_brs": 1,
        "dangling_ar_references": 1,
        "status_counts": {"ready": 1},
        "blocker_counts": {},
    }
    assert execution["complete"] is True
    assert fixer._literals(br_entity, "http://purl.org/dc/terms/title") == [
        "Untouched title"
    ]
    assert sorted(role_uris) == sorted([AR_1, new_ar])
    assert fixer._ids(role_entities[AR_1], fixer.HAS_NEXT) == [new_ar]
    assert fixer._literals(ra_entity, fixer.FOAF_NAME) == ["Bob Smith"]
    assert fixer._ids(id_entity, fixer.USES_IDENTIFIER_SCHEME) == [
        "http://purl.org/spar/datacite/orcid"
    ]
    assert fixer._literals(id_entity, fixer.HAS_LITERAL_VALUE) == [
        "0000-0002-1825-0097"
    ]
    assert len(missing_snapshots) == 2
    assert (
        len(fixer._literals(missing_snapshots[-1], fixer.PROV_INVALIDATED_AT_TIME)) == 1
    )
    assert Path(cast(str, execution["reindex_sentinel"])).exists()
