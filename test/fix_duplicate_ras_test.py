# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv

import orjson
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

    def crossref(self, doi: str) -> fixer.WorkMetadata | None:
        assert doi == "10.1000/example"
        return {
            "identifier": doi,
            "source": "crossref",
            "author": [
                {
                    "family": "Rossi",
                    "given": "Ada",
                    "name": "",
                    "orcid": OLD_ORCID,
                    "identifiers": ({"scheme": "orcid", "value": OLD_ORCID},),
                    "position": 0,
                    "role": "author",
                },
                {
                    "family": "Smith",
                    "given": "John",
                    "name": "",
                    "orcid": None,
                    "identifiers": (),
                    "position": 1,
                    "role": "author",
                },
            ],
            "editor": [],
            "publisher": "",
            "publisher_identifiers": (),
        }

    def datacite(self, doi: str) -> fixer.WorkMetadata | None:
        raise AssertionError(f"Unexpected DataCite request: {doi}")

    def openalex_work(
        self, doi: str = "", openalex_id: str = ""
    ) -> fixer.WorkMetadata | None:
        raise AssertionError(f"Unexpected OpenAlex request: {doi}, {openalex_id}")

    def orcid(self, orcid: str) -> fixer.OrcidProfile | None:
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


def test_count_duplicate_clusters(tmp_path) -> None:
    duplicate_path = tmp_path / "duplicates.csv"
    duplicate_path.write_bytes(
        b"surviving_entity,merged_entities\r\n"
        b"https://example.org/ra/1,https://example.org/ra/2\r\n"
        b"https://example.org/ra/3,https://example.org/ra/4"
    )

    assert fixer._count_duplicate_clusters(str(duplicate_path)) == 2

    duplicate_path.write_bytes(b"surviving_entity,merged_entities\r\n")

    assert fixer._count_duplicate_clusters(str(duplicate_path)) == 0


def test_scan_candidate_clusters_sets_progress_total(tmp_path, monkeypatch) -> None:
    duplicate_path = tmp_path / "duplicates.csv"
    duplicate_path.write_text(
        "surviving_entity,merged_entities\n"
        "https://example.org/ra/1,https://example.org/ra/2\n"
        "https://example.org/ra/3,https://example.org/ra/4\n",
        encoding="utf-8",
    )
    progress_calls = []

    class FakeProgress:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return None

        def add_task(self, description, total):
            progress_calls.append((description, total))
            return 0

        def advance(self, task, advance):
            raise AssertionError("No batches should be processed")

    monkeypatch.setattr(fixer, "create_progress", FakeProgress)
    monkeypatch.setattr(fixer, "iter_cluster_batches", lambda path: iter(()))

    result = fixer.scan_candidate_clusters(
        str(duplicate_path),
        fixer.EntityFileLocator("rdf", 10000, 1000, False),
        1,
    )

    assert progress_calls == [("Checking duplicate clusters", 2)]
    assert result == ([], {}, {}, 0, 0)


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


@pytest.fixture
def audit_dataset(tmp_path):
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
    other_work = g_set.add_br("source", res=f"{BASE}br/0602")
    other_work.create_journal_article()
    other_doi = g_set.add_id("source", res=f"{BASE}id/0603")
    other_doi.create_doi("10.1000/unused")
    other_work.has_identifier(other_doi)
    other_role = g_set.add_ar("source", res=f"{BASE}ar/0603")
    other_role.create_author()
    other_role.is_held_by(second_agent)
    other_work.has_contributor(other_role)
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
    return config_path, duplicate_path, report_path, review_path, cache_path


def test_repair_confirms_role_and_stops_after_first_sufficient_source(
    audit_dataset, monkeypatch
):
    config_path, duplicate_path, report_path, review_path, cache_path = audit_dataset
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
        "",
    )

    assert report["summary"] == {
        "total_clusters": 1,
        "total_cluster_members": 2,
        "candidate_clusters": 1,
        "candidate_agents": 2,
        "locally_consistent_clusters": 0,
        "selected_works": 2,
        "risk_counts": {"conflicting_names": 1},
        "identifier_status_counts": {
            "manual_review": 1,
            "verified_wrong": 1,
        },
        "operation_counts": {"detach_identifier": 1},
    }
    assessments = report["identifier_assessments"]
    assert isinstance(assessments, list)
    assert [assessment["status"] for assessment in assessments] == [
        "manual_review",
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
        "multiple_orcid_values",
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
        agents,
        {(RA_1, INVALID_ORCID): [evidence]},
        {},
        {},
        {},
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
        agents,
        {},
        {},
        {},
        {},
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
        agents,
        {(RA_1, OLD_ORCID): [evidence]},
        {OLD_ORCID: [PersonName(name="Ada Rossi")]},
        {},
        {OLD_ORCID: ProfileClient().orcid(OLD_ORCID)},
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
            "ra": RA_1,
            "action": "detach_identifier",
            "identifier_uri": ID_1,
            "old_value": OLD_ORCID,
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
    assert fixer._preflight_operations(cast(fixer.MetaEditor, editor), [operation]) == {
        RA_1,
        ID_1,
    }

    current[(AR_1, fixer.IS_HELD_BY)] = [RA_2]
    with pytest.raises(
        RuntimeError,
        match=f"Stale plan: {AR_1} is no longer held by {RA_1}",
    ):
        fixer._preflight_operations(cast(fixer.MetaEditor, editor), [operation])


def test_apply_operation_group_detaches_identifier(monkeypatch) -> None:
    editor = FakeEditor()
    operation = fixer._operation(
        "detach_identifier",
        2,
        "Wrong ORCID",
        0.95,
        ra=RA_1,
        identifier_uri=ID_1,
        old_value=OLD_ORCID,
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
        assert uris == {RA_1, ID_1}
        ra = g_set.add_ra(editor.resp_agent, res=RA_1)
        old_identifier = g_set.add_id(editor.resp_agent, res=ID_1)
        old_identifier.create_orcid(OLD_ORCID)
        ra.has_identifier(old_identifier)

    monkeypatch.setattr(fixer, "_current_objects", current_objects)
    monkeypatch.setattr(fixer, "_import_entities", import_entities)
    fixer._apply_operation_group(cast(fixer.MetaEditor, editor), [operation])

    assert editor.saved is not None
    g_set, supplier_prefix = editor.saved
    assert supplier_prefix == "060"
    assert _objects(g_set, RA_1, fixer.HAS_IDENTIFIER) == []


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
        "detach_identifier",
        2,
        "Wrong ORCID",
        0.9,
        ra=RA_2,
        identifier_uri=ID_1,
        old_value=OLD_ORCID,
        evidence=[_evidence_link(RA_2, AR_2)],
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


@pytest.mark.parametrize("profile_name", [None, "Someone Else"])
def test_repair_skips_work_scans_without_a_profile_conflict(
    audit_dataset, monkeypatch, profile_name
):
    config_path, duplicate_path, report_path, review_path, cache_path = audit_dataset
    calls = []

    class UnresolvedClient(AuditClient):
        def orcid(self, orcid: str) -> fixer.OrcidProfile | None:
            calls.append(orcid)
            if profile_name is None:
                return None
            return {"orcid": orcid, "name": profile_name, "given": "", "family": ""}

    def unexpected_context(*args):
        raise AssertionError("No work context should be loaded")

    monkeypatch.setattr(fixer, "AgentMetadataClient", UnresolvedClient)
    monkeypatch.setattr(fixer, "build_context", unexpected_context)
    report = fixer.analyze_duplicate_ras(
        str(config_path),
        str(duplicate_path),
        str(report_path),
        str(review_path),
        str(cache_path),
        "audit@example.org",
        1,
        5,
        False,
        "",
    )
    assert calls == [OLD_ORCID]
    assert report["operations"] == []
    summary = cast(dict[str, object], report["summary"])
    assert summary["selected_works"] == 0
    assert summary["identifier_status_counts"] == {"manual_review": 2}


@pytest.mark.parametrize("crossref_missing", [True, False])
def test_repair_uses_another_source_only_for_unresolved_evidence(
    audit_dataset, monkeypatch, crossref_missing
):
    config_path, duplicate_path, report_path, review_path, cache_path = audit_dataset
    calls = []

    class ProgressiveClient(AuditClient):
        def orcid(self, orcid):
            calls.append("orcid")
            return super().orcid(orcid)

        def crossref(self, doi):
            calls.append("crossref")
            if crossref_missing:
                return None
            work = super().crossref(doi)
            assert work is not None
            work["author"] = []
            return work

        def datacite(self, doi):
            calls.append("datacite")
            work = super().crossref(doi)
            assert work is not None
            work["source"] = "datacite"
            return work

        def openalex_work(self, doi="", openalex_id=""):
            calls.append("openalex")
            work = super().crossref(doi)
            assert work is not None
            work["source"] = "openalex"
            return work

    monkeypatch.setattr(fixer, "AgentMetadataClient", ProgressiveClient)
    report = fixer.analyze_duplicate_ras(
        str(config_path),
        str(duplicate_path),
        str(report_path),
        str(review_path),
        str(cache_path),
        "audit@example.org",
        1,
        5,
        False,
        "",
    )
    assert calls == [
        "orcid",
        "crossref",
        "datacite" if crossref_missing else "openalex",
    ]
    summary = cast(dict[str, object], report["summary"])
    operations = cast(list[dict[str, object]], report["operations"])
    assert summary["operation_counts"] == {"detach_identifier": 1}
    assert operations[0]["ra"] == RA_2


def test_local_selection_writes_only_consistent_clusters(audit_dataset, monkeypatch):
    config_path, duplicate_path, report_path, review_path, cache_path = audit_dataset
    merge_path = config_path.parent / "merge.csv"
    identifier_specs = {
        ID_1: ("orcid", OLD_ORCID),
        ID_2: ("orcid", f"https://orcid.org/{OLD_ORCID}"),
        f"{BASE}id/0603": ("wikidata", "Q1"),
        f"{BASE}id/0604": ("wikidata", "Q2"),
        f"{BASE}id/0605": ("orcid", INVALID_ORCID),
    }
    group_specs = [
        [("Ada Rossi", [ID_1]), ("Ada Rossi", [ID_2])],
        [("Ada Rossi", [ID_1]), ("John Smith", [ID_1])],
        [
            ("Ada Rossi", [ID_1, f"{BASE}id/0603"]),
            ("Ada Rossi", [ID_1, f"{BASE}id/0604"]),
        ],
        [("Ada Rossi", []), ("Ada Rossi", [])],
        [("", [ID_1]), ("", [ID_1])],
        [("Ada Rossi", [ID_1])] * 50,
        [("Ada Rossi", [f"{BASE}id/0605"])] * 2,
    ]
    entities = []
    rows = []
    for group in group_specs:
        members = []
        for name, identifiers in group:
            uri = f"{BASE}ra/060{len(entities) + 1}"
            members.append(uri)
            entities.append(
                {
                    "@id": uri,
                    fixer.FOAF_NAME: [{"@value": name}],
                    fixer.HAS_IDENTIFIER: [
                        {"@id": identifier} for identifier in identifiers
                    ],
                }
            )
        rows.append((members[0], ";".join(members[1:])))
    rdf_dir = config_path.parent / "rdf"
    (rdf_dir / "ra/060/10000/1000.json").write_bytes(
        orjson.dumps([{"@graph": entities}])
    )
    (rdf_dir / "id/060/10000/1000.json").write_bytes(
        orjson.dumps(
            [
                {
                    "@graph": [
                        {
                            "@id": uri,
                            fixer.USES_IDENTIFIER_SCHEME: [
                                {"@id": f"{fixer.DATACITE_PREFIX}{scheme}"}
                            ],
                            fixer.HAS_LITERAL_VALUE: [{"@value": value}],
                        }
                        for uri, (scheme, value) in identifier_specs.items()
                    ]
                }
            ]
        )
    )
    with duplicate_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["surviving_entity", "merged_entities"])
        writer.writerows(rows)

    def unexpected_work(*args, **kwargs):
        raise AssertionError("Local selection must only read agents and identifiers")

    monkeypatch.setattr(fixer, "build_context", unexpected_work)
    monkeypatch.setattr(fixer, "load_provenance", unexpected_work)
    monkeypatch.setattr(fixer, "AgentMetadataClient", unexpected_work)
    monkeypatch.setattr(fixer, "datetime", FixedDateTime)
    result = fixer.select_mergeable_clusters(
        str(config_path),
        str(duplicate_path),
        str(report_path),
        str(merge_path),
        str(review_path),
        1,
    )
    expected_merge = "surviving_entity,merged_entities\n" + "".join(
        f"{rows[index][0]},{rows[index][1]}\n" for index in [0, 5]
    )
    reasons = [
        "conflicting_names",
        "multiple_wikidata_values",
        "no_common_identifier",
        "missing_name",
        "invalid_orcid",
    ]
    expected_review = "surviving_entity,merged_entities,risks\n" + "".join(
        f"{rows[index][0]},{rows[index][1]},{reason}\n"
        for index, reason in zip([1, 2, 3, 4, 6], reasons)
    )
    assert merge_path.read_text() == expected_merge
    assert review_path.read_text() == expected_review
    assert result == {
        "mode": "local_selection",
        "complete": True,
        "generated_at": "2026-07-12T10:30:00+00:00",
        "config": str(config_path),
        "config_sha256": fixer._sha256(str(config_path)),
        "duplicates": str(duplicate_path),
        "duplicates_sha256": fixer._sha256(str(duplicate_path)),
        "merge_file": str(merge_path),
        "review_file": str(review_path),
        "summary": {
            "total_clusters": 7,
            "mergeable_clusters": 2,
            "deferred_clusters": 5,
            "risk_counts": {reason: 1 for reason in reasons},
        },
    }
    assert fixer._read_json_object(str(report_path)) == result
    assert cache_path.exists() is False
    assert [
        [cluster.members for cluster in batch]
        for batch in fixer.iter_cluster_batches(str(review_path))
    ] == [
        [
            tuple([rows[index][0], *rows[index][1].split(";")])
            for index in [1, 2, 3, 4, 6]
        ]
    ]


@pytest.mark.parametrize("interrupted", [True, False])
def test_local_selection_keeps_outputs_on_interruption_or_failure(
    audit_dataset, monkeypatch, interrupted
):
    config_path, duplicate_path, report_path, review_path, _ = audit_dataset
    merge_path = config_path.parent / "merge.csv"
    for path in (report_path, review_path, merge_path):
        path.write_text("existing output\n")
    original_load = fixer.load_agents

    def load_agents(*args):
        if not interrupted:
            raise ValueError("RDF entities not found")
        agents = original_load(*args)
        fixer._handle_signal(2, None)
        return agents

    monkeypatch.setattr(fixer, "load_agents", load_agents)
    monkeypatch.setattr(fixer, "_stop_requested", False)
    with pytest.raises(InterruptedError if interrupted else ValueError):
        fixer.select_mergeable_clusters(
            str(config_path),
            str(duplicate_path),
            str(report_path),
            str(merge_path),
            str(review_path),
            1,
        )
    assert [path.read_text() for path in (report_path, review_path, merge_path)] == [
        "existing output\n"
    ] * 3


def test_repair_skips_api_work_requests_for_invalid_local_chains(
    audit_dataset, monkeypatch
):
    config_path, duplicate_path, report_path, review_path, cache_path = audit_dataset
    role_path = config_path.parent / "rdf/ar/060/10000/1000.json"
    graphs = orjson.loads(role_path.read_bytes())
    for graph in graphs:
        for entity in graph["@graph"]:
            if entity["@id"] == AR_1:
                entity[fixer.HAS_NEXT] = [{"@id": f"{BASE}ar/06999"}]
    role_path.write_bytes(orjson.dumps(graphs))
    calls = []

    class InvalidChainClient(AuditClient):
        def orcid(self, orcid):
            calls.append(orcid)
            return super().orcid(orcid)

        def crossref(self, doi):
            raise AssertionError("Invalid local chain cannot confirm responsibility")

    monkeypatch.setattr(fixer, "AgentMetadataClient", InvalidChainClient)
    report = fixer.analyze_duplicate_ras(
        str(config_path),
        str(duplicate_path),
        str(report_path),
        str(review_path),
        str(cache_path),
        "audit@example.org",
        1,
        1,
        False,
        "",
    )
    assert calls == [OLD_ORCID]
    assert report["operations"] == []
    assert report["role_assessments"] == [
        {
            "br": BR_1,
            "role": "author",
            "source": "",
            "reason": "invalid_local_chain",
            "chain_status": "dangling_or_cross_role",
            "ambiguous": True,
            "pairs": [],
            "unmatched_local": [AR_1, AR_2],
            "unmatched_external": [],
        }
    ]
