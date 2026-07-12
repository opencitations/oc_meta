# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
from datetime import datetime, timezone
from typing import cast

import pytest
import yaml
from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet

from oc_meta.lib.agent_matching import PersonName
from oc_meta.run.patches import fix_duplicate_ras as fixer

BASE = "https://w3id.org/oc/meta/"
RA_1 = f"{BASE}ra/0601"
RA_2 = f"{BASE}ra/0602"
RA_3 = f"{BASE}ra/0603"
AR_1 = f"{BASE}ar/0601"
AR_2 = f"{BASE}ar/0602"
BR_1 = f"{BASE}br/0601"
ID_1 = f"{BASE}id/0601"
ID_2 = f"{BASE}id/0602"
OLD_ORCID = "0000-0002-8420-0696"
NEW_ORCID = "0000-0002-1825-0097"
INVALID_ORCID = "0000-0002-8420-0697"


class FakeEditor:
    def __init__(self) -> None:
        self.base_iri = BASE
        self.endpoint = "https://example.org/sparql"
        self.resp_agent = "https://orcid.org/0000-0002-8420-0696"
        self.counter_handler = None
        self.supplier_prefix = "060"
        self.saved: tuple[GraphSet, str] | None = None

    def save(self, g_set: GraphSet, supplier_prefix: str) -> None:
        self.saved = (g_set, supplier_prefix)


class NoApiClient:
    def orcid(self, orcid: str) -> None:
        raise AssertionError(f"Unexpected ORCID request: {orcid}")


class ProfileClient:
    def orcid(self, orcid: str) -> fixer.OrcidProfile:
        assert orcid == OLD_ORCID
        return {
            "orcid": OLD_ORCID,
            "given": "John",
            "family": "Smith",
            "name": "John Smith",
        }


class AuditClient:
    def __init__(
        self,
        mailto: str,
        cache: fixer.ApiCache,
        refresh_cache: bool,
        openalex_api_key: str,
    ) -> None:
        assert mailto == "audit@example.org"
        assert isinstance(cache, fixer.ApiCache)
        assert refresh_cache is False
        assert openalex_api_key == ""

    def work_sources(self, doi: str, openalex_id: str) -> list[fixer.WorkMetadata]:
        assert doi == "10.1000/example"
        assert openalex_id == ""
        return [
            {
                "identifier": doi,
                "source": "crossref",
                "author": [
                    {
                        "family": "Rossi",
                        "given": "Ada",
                        "name": "",
                        "orcid": OLD_ORCID,
                        "position": 0,
                        "role": "author",
                    },
                    {
                        "family": "Smith",
                        "given": "John",
                        "name": "",
                        "orcid": None,
                        "position": 1,
                        "role": "author",
                    },
                ],
                "editor": [],
                "publisher": "",
            }
        ]

    def orcid(self, orcid: str) -> fixer.OrcidProfile:
        assert orcid == OLD_ORCID
        return {
            "orcid": OLD_ORCID,
            "given": "Ada",
            "family": "Rossi",
            "name": "Ada Rossi",
        }

    def close(self) -> None:
        return None


class FixedDateTime:
    @classmethod
    def now(cls, zone: timezone) -> datetime:
        assert zone is timezone.utc
        return datetime(2026, 7, 12, 10, 30, tzinfo=timezone.utc)


def _agent(
    uri: str,
    name: str,
    identifiers: tuple[fixer.IdentifierInfo, ...] = (),
) -> fixer.AgentInfo:
    return fixer.AgentInfo(uri, PersonName(name=name), identifiers)


def _evidence_link(ra: str, ar: str, next_uri: str = "") -> dict[str, str]:
    return {
        "br": BR_1,
        "ar": ar,
        "ra": ra,
        "next": next_uri,
        "work_identifier_uri": ID_2,
        "work_identifier_scheme": "doi",
        "work_identifier_value": "10.1000/example",
    }


def _objects(g_set: GraphSet, uri: str, predicate: str) -> list[str]:
    entity = g_set.get_entity(uri)
    assert entity is not None
    return sorted(
        value.value for _, _, value in entity.g.triples((entity.res, predicate, None))
    )


def test_ordered_chain_classifies_structural_errors() -> None:
    first = fixer.RoleInfo(AR_1, RA_1, "author", (AR_2,))
    second = fixer.RoleInfo(AR_2, RA_2, "author", ())
    valid = fixer.ordered_chain([second, first])
    cycle = fixer.ordered_chain(
        [
            first,
            fixer.RoleInfo(AR_2, RA_2, "author", (AR_1,)),
        ]
    )
    fork = fixer.ordered_chain(
        [fixer.RoleInfo(AR_1, RA_1, "author", (AR_2, f"{BASE}ar/0603"))]
    )
    dangling = fixer.ordered_chain(
        [fixer.RoleInfo(AR_1, RA_1, "author", (f"{BASE}ar/0699",))]
    )
    multiple_holders = fixer.ordered_chain(
        [fixer.RoleInfo(AR_1, RA_1, "author", (), (RA_1, RA_2))]
    )

    assert valid == fixer.OrderedChain("valid", (first, second))
    assert cycle == fixer.OrderedChain(
        "cycle_or_multiple_heads",
        (first, fixer.RoleInfo(AR_2, RA_2, "author", (AR_1,))),
    )
    assert fork == fixer.OrderedChain(
        "fork",
        (fixer.RoleInfo(AR_1, RA_1, "author", (AR_2, f"{BASE}ar/0603")),),
    )
    assert dangling == fixer.OrderedChain(
        "dangling_or_cross_role",
        (fixer.RoleInfo(AR_1, RA_1, "author", (f"{BASE}ar/0699",)),),
    )
    assert multiple_holders == fixer.OrderedChain(
        "multiple_or_missing_holders",
        (fixer.RoleInfo(AR_1, RA_1, "author", (), (RA_1, RA_2)),),
    )


def test_dry_run_confirms_roles_before_proposing_identifier_detach(
    tmp_path, monkeypatch
) -> None:
    rdf_dir = tmp_path / "rdf"
    g_set = GraphSet(BASE, supplier_prefix="060", wanted_label=False)
    first_agent = g_set.add_ra("source", res=RA_1)
    first_agent.has_given_name("Ada")
    first_agent.has_family_name("Rossi")
    second_agent = g_set.add_ra("source", res=RA_2)
    second_agent.has_given_name("John")
    second_agent.has_family_name("Smith")
    shared_orcid = g_set.add_id("source", res=ID_1)
    shared_orcid.create_orcid(OLD_ORCID)
    first_agent.has_identifier(shared_orcid)
    second_agent.has_identifier(shared_orcid)
    work = g_set.add_br("source", res=BR_1)
    work.create_journal_article()
    doi = g_set.add_id("source", res=ID_2)
    doi.create_doi("10.1000/example")
    work.has_identifier(doi)
    first_role = g_set.add_ar("source", res=AR_1)
    first_role.create_author()
    first_role.is_held_by(first_agent)
    second_role = g_set.add_ar("source", res=AR_2)
    second_role.create_author()
    second_role.is_held_by(second_agent)
    first_role.has_next(second_role)
    work.has_contributor(first_role)
    work.has_contributor(second_role)
    Storer(
        g_set,
        dir_split=10000,
        n_file_item=1000,
        zip_output=False,
    ).store_all(f"{rdf_dir}/", BASE)

    config_path = tmp_path / "meta.yaml"
    duplicate_path = tmp_path / "duplicates.csv"
    report_path = tmp_path / "report.json"
    review_path = tmp_path / "review.csv"
    cache_path = tmp_path / "api.sqlite"
    config_path.write_text(
        yaml.safe_dump(
            {
                "base_iri": BASE,
                "base_output_dir": str(tmp_path),
                "output_rdf_dir": str(tmp_path),
                "triplestore_url": "https://example.org/sparql",
                "provenance_triplestore_url": "https://example.org/prov",
                "dir_split_number": 10000,
                "items_per_file": 1000,
                "zip_output_rdf": False,
                "rdf_files_only": True,
                "supplier_prefix": "060",
            }
        ),
        encoding="utf-8",
    )
    duplicate_path.write_text(
        f"surviving_entity,merged_entities\n{RA_1},{RA_2}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(fixer, "AgentMetadataClient", AuditClient)
    report = fixer.analyze_duplicate_ras(
        str(config_path),
        str(duplicate_path),
        str(report_path),
        str(review_path),
        str(cache_path),
        "audit@example.org",
        2,
        5,
        False,
        False,
        "",
    )

    assert report["summary"] == {
        "total_clusters": 1,
        "total_cluster_members": 2,
        "candidate_clusters": 1,
        "candidate_agents": 2,
        "locally_consistent_clusters": 0,
        "selected_works": 1,
        "risk_counts": {"conflicting_names": 1},
        "identifier_status_counts": {
            "verified_correct": 1,
            "verified_wrong": 1,
        },
        "operation_counts": {"detach_identifier": 1},
    }
    assessments = report["identifier_assessments"]
    assert isinstance(assessments, list)
    assert [assessment["status"] for assessment in assessments] == [
        "verified_correct",
        "verified_wrong",
    ]
    operations = report["operations"]
    assert isinstance(operations, list)
    assert operations == [
        fixer._operation(
            "detach_identifier",
            2,
            f"ORCID profile matches {RA_1}, not {RA_2}",
            1.0,
            ra=RA_2,
            identifier_uri=ID_1,
            old_value=OLD_ORCID,
            evidence=[_evidence_link(RA_2, AR_2)],
        )
    ]
    assert fixer._read_json_object(str(report_path)) == report


def test_cluster_risks_detect_names_orcids_and_bridge() -> None:
    cluster = fixer.Cluster(2, RA_1, (RA_1, RA_2))
    agents = {
        RA_1: _agent(
            RA_1,
            "Ada Rossi",
            (
                fixer.IdentifierInfo(ID_1, "orcid", OLD_ORCID),
                fixer.IdentifierInfo(f"{BASE}id/0603", "orcid", NEW_ORCID),
            ),
        ),
        RA_2: _agent(
            RA_2,
            "John Smith",
            (fixer.IdentifierInfo(ID_2, "orcid", NEW_ORCID),),
        ),
    }
    assert fixer.cluster_risks(cluster, agents) == [
        "conflicting_names",
        "multiple_orcids",
        "bridge_agent",
    ]


def test_cluster_risks_detect_conflicting_given_names() -> None:
    cluster = fixer.Cluster(2, RA_1, (RA_1, RA_2))
    shared_orcid = fixer.IdentifierInfo(ID_1, "orcid", OLD_ORCID)
    agents = {
        RA_1: fixer.AgentInfo(
            RA_1, PersonName(given="John", family="Smith"), (shared_orcid,)
        ),
        RA_2: fixer.AgentInfo(
            RA_2, PersonName(given="Jane", family="Smith"), (shared_orcid,)
        ),
    }
    assert fixer.cluster_risks(cluster, agents) == ["conflicting_names"]


def test_role_check_proposes_existing_cluster_agent() -> None:
    cluster = fixer.Cluster(2, RA_1, (RA_1, RA_2))
    role = fixer.RoleInfo(AR_1, RA_1, "author", ())
    external: list[fixer.AgentMetadata] = [
        {
            "family": "Rossi",
            "given": "Ada",
            "name": "",
            "orcid": None,
            "position": 0,
            "role": "author",
        }
    ]
    operations = fixer._role_operations(
        {RA_1: cluster, RA_2: cluster},
        fixer.WorkInfo(BR_1, (), (AR_1,)),
        fixer.OrderedChain("valid", (role,)),
        external,
        {
            RA_1: _agent(RA_1, "John Smith"),
            RA_2: _agent(RA_2, "Ada Rossi"),
        },
        "crossref",
    )
    assert operations == [
        fixer._operation(
            "reassign_role",
            2,
            f"crossref contributor at position 0 matches {RA_2}",
            1.0,
            br=BR_1,
            ar=AR_1,
            ra=RA_1,
            old_value=RA_1,
            new_value=RA_2,
        )
    ]


def test_role_check_reorders_without_reassigning_holders() -> None:
    cluster = fixer.Cluster(2, RA_1, (RA_1, RA_2))
    first = fixer.RoleInfo(AR_1, RA_1, "author", (AR_2,))
    second = fixer.RoleInfo(AR_2, RA_2, "author", ())
    external: list[fixer.AgentMetadata] = [
        {
            "family": "Smith",
            "given": "John",
            "name": "",
            "orcid": None,
            "position": 0,
            "role": "author",
        },
        {
            "family": "Rossi",
            "given": "Ada",
            "name": "",
            "orcid": None,
            "position": 1,
            "role": "author",
        },
    ]
    operations = fixer._role_operations(
        {RA_1: cluster, RA_2: cluster},
        fixer.WorkInfo(BR_1, (), (AR_1, AR_2)),
        fixer.OrderedChain("valid", (first, second)),
        external,
        {
            RA_1: _agent(RA_1, "Ada Rossi"),
            RA_2: _agent(RA_2, "John Smith"),
        },
        "crossref",
    )
    assert operations == [
        fixer._operation(
            "reorder_chain",
            2,
            "crossref confirms the same contributors in a different order",
            0.9,
            br=BR_1,
            links=[
                {"ar": AR_2, "old_next": "", "new_next": AR_1},
                {"ar": AR_1, "old_next": AR_2, "new_next": ""},
            ],
        )
    ]


def test_load_provenance_reports_first_and_latest_snapshot() -> None:
    uri = "https://w3id.org/oc/meta/br/060126"
    result = fixer._load_provenance_batch(
        [
            (
                "test/file_manager/unzipped_dir/060/10000/1000/prov/se.json",
                frozenset({uri}),
            )
        ]
    )
    assert result == {
        uri: {
            "snapshot_count": 2,
            "created_at": "2022-07-28T15:38:17",
            "latest_at": "2022-09-09T10:40:03",
            "latest_snapshot": f"{uri}/prov/se/2",
            "attributed_to": ["https://orcid.org/0000-0002-8420-0696"],
            "primary_sources": ["https://api.crossref.org/"],
            "description": f"The entity '{uri}' has been modified.",
            "update_query": (
                "INSERT DATA { GRAPH <https://w3id.org/oc/meta/br/> { "
                "<https://w3id.org/oc/meta/br/060126> "
                "<http://purl.org/spar/datacite/hasIdentifier> "
                "<https://w3id.org/oc/meta/id/06190168142> .\n"
                "<https://w3id.org/oc/meta/br/060126> "
                "<http://purl.org/spar/datacite/hasIdentifier> "
                "<https://w3id.org/oc/meta/id/06190168141> . } }"
            ),
        }
    }


def test_invalid_orcid_is_detached_only_after_role_confirmation() -> None:
    cluster = fixer.Cluster(2, RA_1, (RA_1, RA_2))
    agents = {
        RA_1: _agent(
            RA_1,
            "Ada Rossi",
            (fixer.IdentifierInfo(ID_1, "orcid", INVALID_ORCID),),
        ),
        RA_2: _agent(RA_2, "John Smith"),
    }
    evidence = fixer.WorkEvidence(
        BR_1,
        AR_1,
        "",
        ID_2,
        "doi",
        "10.1000/example",
        "author",
        "crossref",
        True,
        1.0,
        None,
        "Ada Rossi",
        False,
    )
    assessments, operations = fixer.classify_identifiers(
        [cluster],
        {2: ["conflicting_names"]},
        agents,
        {(RA_1, INVALID_ORCID): [evidence]},
        {},
        {},
        NoApiClient(),
    )
    assert assessments == [
        {
            "csv_row": 2,
            "ra": RA_1,
            "identifier_uri": ID_1,
            "orcid": INVALID_ORCID,
            "status": "verified_wrong",
            "reason": "ORCID has an invalid format or checksum",
            "profile": None,
            "profile_score": 0.0,
            "best_other_ra": "",
            "best_other_score": 0.0,
            "replacement_orcid": None,
            "work_evidence": [
                {
                    "br": BR_1,
                    "ar": AR_1,
                    "next_uri": "",
                    "work_identifier_uri": ID_2,
                    "work_identifier_scheme": "doi",
                    "work_identifier_value": "10.1000/example",
                    "role": "author",
                    "source": "crossref",
                    "matched": True,
                    "name_score": 1.0,
                    "api_orcid": None,
                    "api_name": "Ada Rossi",
                    "contested_elsewhere": False,
                }
            ],
            "agent_provenance": None,
            "identifier_provenance": None,
        }
    ]
    assert operations == [
        fixer._operation(
            "detach_identifier",
            2,
            "ORCID has an invalid format or checksum",
            0.9,
            ra=RA_1,
            identifier_uri=ID_1,
            old_value=INVALID_ORCID,
            evidence=[_evidence_link(RA_1, AR_1)],
        )
    ]

    unconfirmed_assessments, unconfirmed_operations = fixer.classify_identifiers(
        [cluster],
        {2: ["conflicting_names"]},
        agents,
        {},
        {},
        {},
        NoApiClient(),
    )
    assert unconfirmed_assessments == [
        {
            **assessments[0],
            "status": "manual_review",
            "reason": (
                "ORCID has an invalid format or checksum, but the local work "
                "responsibility is not externally confirmed"
            ),
            "work_evidence": [],
        }
    ]
    assert unconfirmed_operations == []


def test_orcid_profile_overrides_contaminated_work_identifier() -> None:
    cluster = fixer.Cluster(2, RA_1, (RA_1, RA_2))
    shared_identifier = fixer.IdentifierInfo(ID_1, "orcid", OLD_ORCID)
    agents = {
        RA_1: _agent(RA_1, "Ada Rossi", (shared_identifier,)),
        RA_2: _agent(RA_2, "John Smith", (shared_identifier,)),
    }
    evidence = fixer.WorkEvidence(
        BR_1,
        AR_1,
        "",
        ID_2,
        "doi",
        "10.1000/example",
        "author",
        "crossref",
        True,
        1.0,
        OLD_ORCID,
        "Ada Rossi",
        False,
    )
    assessments, operations = fixer.classify_identifiers(
        [cluster],
        {2: ["conflicting_names"]},
        agents,
        {(RA_1, OLD_ORCID): [evidence]},
        {OLD_ORCID: [PersonName(name="Ada Rossi")]},
        {},
        ProfileClient(),
    )
    assert [assessment["status"] for assessment in assessments] == [
        "verified_wrong",
        "manual_review",
    ]
    assert operations == [
        fixer._operation(
            "detach_identifier",
            2,
            f"ORCID profile matches {RA_2}, not {RA_1}",
            1.0,
            ra=RA_1,
            identifier_uri=ID_1,
            old_value=OLD_ORCID,
            evidence=[_evidence_link(RA_1, AR_1)],
        )
    ]


def test_review_csv_accepts_only_decisions(tmp_path) -> None:
    operation = fixer._operation(
        "detach_identifier",
        2,
        "Wrong ORCID",
        0.9,
        ra=RA_1,
        identifier_uri=ID_1,
        old_value=OLD_ORCID,
    )
    review_path = tmp_path / "review.csv"
    fixer.write_review_file(str(review_path), [operation])
    with open(review_path, newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert rows == [
        {
            "operation_id": operation["operation_id"],
            "csv_row": "2",
            "br": "",
            "ar": "",
            "ra": RA_1,
            "action": "detach_identifier",
            "identifier_uri": ID_1,
            "old_value": OLD_ORCID,
            "new_value": "",
            "confidence": "0.9",
            "reason": "Wrong ORCID",
            "decision": "",
        }
    ]
    rows[0]["decision"] = "approve"
    with open(review_path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fixer.REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    assert fixer.read_review_decisions(str(review_path), [operation]) == [operation]

    rows[0]["reason"] = "Changed reason"
    with open(review_path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fixer.REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(
        ValueError,
        match=rf"Review row {operation['operation_id']} differs from the plan in: \['reason'\]",
    ):
        fixer.read_review_decisions(str(review_path), [operation])


def test_identifier_preflight_rechecks_confirmed_work_role(monkeypatch) -> None:
    editor = FakeEditor()
    operation = fixer._operation(
        "detach_identifier",
        2,
        "Wrong ORCID",
        0.9,
        ra=RA_1,
        identifier_uri=ID_1,
        old_value=OLD_ORCID,
        evidence=[_evidence_link(RA_1, AR_1, AR_2)],
    )
    current = {
        (RA_1, fixer.HAS_IDENTIFIER): [ID_1],
        (ID_1, fixer.USES_IDENTIFIER_SCHEME): [f"{fixer.DATACITE_PREFIX}orcid"],
        (ID_1, fixer.HAS_LITERAL_VALUE): [OLD_ORCID],
        (BR_1, fixer.HAS_IDENTIFIER): [ID_2],
        (ID_2, fixer.USES_IDENTIFIER_SCHEME): [f"{fixer.DATACITE_PREFIX}doi"],
        (ID_2, fixer.HAS_LITERAL_VALUE): ["10.1000/example"],
        (BR_1, fixer.IS_DOCUMENT_CONTEXT_FOR): [AR_1],
        (AR_1, fixer.IS_HELD_BY): [RA_1],
        (AR_1, fixer.HAS_NEXT): [AR_2],
    }

    def current_objects(endpoint: str, subject: str, predicate: str) -> list[str]:
        assert endpoint == editor.endpoint
        return current[(subject, predicate)]

    monkeypatch.setattr(fixer, "_current_objects", current_objects)
    assert fixer._preflight_operations(cast(fixer.MetaEditor, editor), [operation]) == (
        {RA_1, ID_1},
        {},
    )

    current[(AR_1, fixer.IS_HELD_BY)] = [RA_2]
    with pytest.raises(
        RuntimeError,
        match=f"Stale plan: {AR_1} is no longer held by {RA_1}",
    ):
        fixer._preflight_operations(cast(fixer.MetaEditor, editor), [operation])


def test_apply_operation_group_replaces_existing_identifier(monkeypatch) -> None:
    editor = FakeEditor()
    operation = fixer._operation(
        "replace_identifier",
        2,
        "Wrong ORCID",
        0.95,
        ra=RA_1,
        identifier_uri=ID_1,
        old_value=OLD_ORCID,
        new_value=NEW_ORCID,
    )
    current = {
        (RA_1, fixer.HAS_IDENTIFIER): [ID_1],
        (ID_1, fixer.USES_IDENTIFIER_SCHEME): [f"{fixer.DATACITE_PREFIX}orcid"],
        (ID_1, fixer.HAS_LITERAL_VALUE): [OLD_ORCID],
    }

    def current_objects(endpoint: str, subject: str, predicate: str) -> list[str]:
        assert endpoint == editor.endpoint
        return current[(subject, predicate)]

    def import_entities(
        imported_editor: FakeEditor, g_set: GraphSet, uris: set[str]
    ) -> None:
        assert imported_editor is editor
        assert uris == {RA_1, ID_1, ID_2}
        ra = g_set.add_ra(editor.resp_agent, res=RA_1)
        old_identifier = g_set.add_id(editor.resp_agent, res=ID_1)
        old_identifier.create_orcid(OLD_ORCID)
        replacement = g_set.add_id(editor.resp_agent, res=ID_2)
        replacement.create_orcid(NEW_ORCID)
        ra.has_identifier(old_identifier)

    monkeypatch.setattr(fixer, "_current_objects", current_objects)
    monkeypatch.setattr(fixer, "_find_orcid_identifier", lambda endpoint, value: ID_2)
    monkeypatch.setattr(fixer, "_import_entities", import_entities)
    fixer._apply_operation_group(cast(fixer.MetaEditor, editor), [operation])

    assert editor.saved is not None
    g_set, supplier_prefix = editor.saved
    assert supplier_prefix == "060"
    assert _objects(g_set, RA_1, fixer.HAS_IDENTIFIER) == [ID_2]


def test_shared_replacement_orcid_creates_one_identifier(monkeypatch) -> None:
    editor = FakeEditor()
    first_old_id = f"{BASE}id/06101"
    second_old_id = f"{BASE}id/06102"
    second_old_orcid = "0000-0002-1694-233X"
    operations = [
        fixer._operation(
            "replace_identifier",
            2,
            "Wrong ORCID",
            0.95,
            ra=RA_1,
            identifier_uri=first_old_id,
            old_value=OLD_ORCID,
            new_value=NEW_ORCID,
        ),
        fixer._operation(
            "replace_identifier",
            2,
            "Wrong ORCID",
            0.95,
            ra=RA_2,
            identifier_uri=second_old_id,
            old_value=second_old_orcid,
            new_value=NEW_ORCID,
        ),
    ]
    current = {
        (RA_1, fixer.HAS_IDENTIFIER): [first_old_id],
        (RA_2, fixer.HAS_IDENTIFIER): [second_old_id],
        (first_old_id, fixer.USES_IDENTIFIER_SCHEME): [f"{fixer.DATACITE_PREFIX}orcid"],
        (second_old_id, fixer.USES_IDENTIFIER_SCHEME): [
            f"{fixer.DATACITE_PREFIX}orcid"
        ],
        (first_old_id, fixer.HAS_LITERAL_VALUE): [OLD_ORCID],
        (second_old_id, fixer.HAS_LITERAL_VALUE): [second_old_orcid],
    }
    replacement_queries = []

    def current_objects(endpoint: str, subject: str, predicate: str) -> list[str]:
        assert endpoint == editor.endpoint
        return current[(subject, predicate)]

    def find_identifier(endpoint: str, value: str) -> str:
        assert endpoint == editor.endpoint
        replacement_queries.append(value)
        return ""

    def import_entities(
        imported_editor: FakeEditor, g_set: GraphSet, uris: set[str]
    ) -> None:
        assert imported_editor is editor
        assert uris == {RA_1, RA_2, first_old_id, second_old_id}
        first_agent = g_set.add_ra(editor.resp_agent, res=RA_1)
        second_agent = g_set.add_ra(editor.resp_agent, res=RA_2)
        first_identifier = g_set.add_id(editor.resp_agent, res=first_old_id)
        first_identifier.create_orcid(OLD_ORCID)
        second_identifier = g_set.add_id(editor.resp_agent, res=second_old_id)
        second_identifier.create_orcid(second_old_orcid)
        first_agent.has_identifier(first_identifier)
        second_agent.has_identifier(second_identifier)

    monkeypatch.setattr(fixer, "_current_objects", current_objects)
    monkeypatch.setattr(fixer, "_find_orcid_identifier", find_identifier)
    monkeypatch.setattr(fixer, "_import_entities", import_entities)
    groups = fixer._operation_groups(operations)
    sorted_operations = sorted(
        operations, key=lambda operation: str(operation["operation_id"])
    )
    expected_group_id = fixer._operation_id(
        "group",
        *(str(operation["operation_id"]) for operation in sorted_operations),
    )
    assert groups == [
        (
            expected_group_id,
            sorted_operations,
        )
    ]
    fixer._apply_operation_group(cast(fixer.MetaEditor, editor), groups[0][1])

    assert replacement_queries == [NEW_ORCID]
    assert editor.saved is not None
    g_set, supplier_prefix = editor.saved
    first_identifiers = _objects(g_set, RA_1, fixer.HAS_IDENTIFIER)
    second_identifiers = _objects(g_set, RA_2, fixer.HAS_IDENTIFIER)
    assert supplier_prefix == "060"
    assert first_identifiers == second_identifiers
    assert len(first_identifiers) == 1


def test_apply_operation_group_reassigns_agent_and_reorders_chain(monkeypatch) -> None:
    editor = FakeEditor()
    reassign = fixer._operation(
        "reassign_role",
        2,
        "Crossref confirms another agent",
        1.0,
        br=BR_1,
        ar=AR_1,
        ra=RA_1,
        old_value=RA_1,
        new_value=RA_2,
    )
    reorder = fixer._operation(
        "reorder_chain",
        2,
        "Crossref confirms another order",
        0.9,
        br=BR_1,
        links=[
            {"ar": AR_2, "old_next": "", "new_next": AR_1},
            {"ar": AR_1, "old_next": AR_2, "new_next": ""},
        ],
    )
    current = {
        (BR_1, fixer.IS_DOCUMENT_CONTEXT_FOR): [AR_1, AR_2],
        (AR_1, fixer.IS_HELD_BY): [RA_1],
        (AR_1, fixer.HAS_NEXT): [AR_2],
        (AR_2, fixer.HAS_NEXT): [],
    }

    def current_objects(endpoint: str, subject: str, predicate: str) -> list[str]:
        assert endpoint == editor.endpoint
        return current[(subject, predicate)]

    def import_entities(
        imported_editor: FakeEditor, g_set: GraphSet, uris: set[str]
    ) -> None:
        assert imported_editor is editor
        assert uris == {AR_1, AR_2, RA_1, RA_2}
        old_agent = g_set.add_ra(editor.resp_agent, res=RA_1)
        g_set.add_ra(editor.resp_agent, res=RA_2)
        first = g_set.add_ar(editor.resp_agent, res=AR_1)
        second = g_set.add_ar(editor.resp_agent, res=AR_2)
        first.is_held_by(old_agent)
        first.has_next(second)

    monkeypatch.setattr(fixer, "_current_objects", current_objects)
    monkeypatch.setattr(fixer, "_import_entities", import_entities)
    fixer._apply_operation_group(cast(fixer.MetaEditor, editor), [reassign, reorder])

    assert editor.saved is not None
    g_set, supplier_prefix = editor.saved
    assert supplier_prefix == "060"
    assert _objects(g_set, AR_1, fixer.IS_HELD_BY) == [RA_2]
    assert _objects(g_set, AR_1, fixer.HAS_NEXT) == []
    assert _objects(g_set, AR_2, fixer.HAS_NEXT) == [AR_1]


def test_execute_plan_applies_only_approved_operations_and_writes_sentinel(
    tmp_path, monkeypatch
) -> None:
    config_path = tmp_path / "meta.yaml"
    duplicate_path = tmp_path / "duplicates.csv"
    plan_path = tmp_path / "plan.json"
    review_path = tmp_path / "review.csv"
    progress_path = tmp_path / "progress.json"
    execution_path = tmp_path / "execution.json"
    config_path.write_text("base_output_dir: output\n", encoding="utf-8")
    duplicate_path.write_text(
        f"surviving_entity,merged_entities\n{RA_1},{RA_2}\n",
        encoding="utf-8",
    )
    approved_operation = fixer._operation(
        "detach_identifier",
        2,
        "Wrong ORCID",
        0.9,
        ra=RA_1,
        identifier_uri=ID_1,
        old_value=OLD_ORCID,
        evidence=[_evidence_link(RA_1, AR_1, AR_2)],
    )
    rejected_operation = fixer._operation(
        "reassign_role",
        2,
        "Wrong agent",
        0.9,
        br=BR_1,
        ar=AR_1,
        ra=RA_1,
        old_value=RA_1,
        new_value=RA_2,
    )
    plan = {
        "schema_version": fixer.PLAN_SCHEMA_VERSION,
        "complete": True,
        "config_sha256": fixer._sha256(str(config_path)),
        "duplicates": str(duplicate_path),
        "duplicates_sha256": fixer._sha256(str(duplicate_path)),
        "review_file": str(review_path),
        "operations": [approved_operation, rejected_operation],
    }
    fixer._write_json(str(plan_path), plan)
    fixer.write_review_file(str(review_path), [approved_operation, rejected_operation])
    with open(review_path, newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["decision"] = "approve"
    rows[1]["decision"] = "reject"
    with open(review_path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fixer.REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    applied = []

    class FakeMetaEditor(FakeEditor):
        def __init__(
            self, received_config: str, resp_agent: str, save_queries: bool
        ) -> None:
            assert received_config == str(config_path)
            assert resp_agent == "https://orcid.org/0000-0002-8420-0696"
            assert save_queries is True
            super().__init__()

    def apply_operation_group(
        editor: FakeMetaEditor,
        operations: list[dict[str, object]],
    ) -> None:
        assert isinstance(editor, FakeMetaEditor)
        applied.append(operations)

    monkeypatch.setattr(fixer, "MetaEditor", FakeMetaEditor)
    monkeypatch.setattr(fixer, "_apply_operation_group", apply_operation_group)
    monkeypatch.setattr(fixer, "datetime", FixedDateTime)
    result = fixer.execute_plan(
        str(config_path),
        str(plan_path),
        None,
        "https://orcid.org/0000-0002-8420-0696",
        str(progress_path),
        str(execution_path),
    )
    sentinel_path = tmp_path / fixer.REINDEX_SENTINEL_FILENAME
    plan_hash = fixer._sha256(str(plan_path))
    group_id = fixer._operation_id("group", str(approved_operation["operation_id"]))
    assert applied == [[approved_operation]]
    assert result == {
        "schema_version": fixer.PLAN_SCHEMA_VERSION,
        "plan": str(plan_path),
        "plan_sha256": plan_hash,
        "review_file": str(review_path),
        "review_sha256": fixer._sha256(str(review_path)),
        "generated_at": "2026-07-12T10:30:00+00:00",
        "complete": True,
        "approved_operations": 1,
        "total_groups": 1,
        "completed_groups": [group_id],
        "reindex_sentinel": str(sentinel_path),
    }
    assert fixer._read_json_object(str(execution_path)) == result
    assert progress_path.exists() is False
    assert sentinel_path.read_text(encoding="utf-8") == (
        f"{plan_path} changed RDF files on top of the current triplestore snapshot.\n"
        "Re-index the triplestore from the RDF files, then delete this file before "
        "another correction or merge run. Do not reuse the original duplicate CSV: "
        "run duplicate detection again.\n"
    )
