# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import os

import orjson
import pytest
import yaml
from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.patches.fix_duplicate_part_of import (
    FRBR_PART_OF,
    ResolvedCase,
    _build_report,
    _follow_to_venue,
    _load_progress,
    _read_entity,
    _save_progress,
    _scan_br_batch,
    build_chain_map,
    check_orphans,
    enrich_manual_review,
    fix_br_part_of,
    resolve_cases,
    scan_duplicate_part_of,
)

RESP_AGENT = "https://w3id.org/oc/meta/prov/pa/1"
BASE_IRI = "https://w3id.org/oc/meta/"
SUPPLIER_PREFIX = "060"
DIR_SPLIT = 10000
ITEMS_PER_FILE = 1000

BR1_URI = "https://w3id.org/oc/meta/br/0601"
ISSUE_A_URI = "https://w3id.org/oc/meta/br/0602"
ISSUE_B_URI = "https://w3id.org/oc/meta/br/0603"
VOLUME_A_URI = "https://w3id.org/oc/meta/br/0604"
VOLUME_B_URI = "https://w3id.org/oc/meta/br/0605"
JOURNAL_URI = "https://w3id.org/oc/meta/br/0606"
JOURNAL_B_URI = "https://w3id.org/oc/meta/br/0607"
ID1_URI = "https://w3id.org/oc/meta/id/0601"

EXPRESSION = "http://purl.org/spar/fabio/Expression"
JOURNAL_ARTICLE = "http://purl.org/spar/fabio/JournalArticle"
JOURNAL_ISSUE = "http://purl.org/spar/fabio/JournalIssue"
JOURNAL_VOLUME = "http://purl.org/spar/fabio/JournalVolume"
JOURNAL_TYPE = "http://purl.org/spar/fabio/Journal"
REFERENCE_ENTRY = "http://purl.org/spar/fabio/ReferenceEntry"
REFERENCE_BOOK = "http://purl.org/spar/fabio/ReferenceBook"
PROCEEDINGS = "http://purl.org/spar/fabio/AcademicProceedings"
PROCEEDINGS_PAPER = "http://purl.org/spar/fabio/ProceedingsPaper"


def _make_entity(uri, types, title=None, part_of=None, id_uris=None):
    entity: dict = {"@id": uri, "@type": types}
    if title:
        entity["http://purl.org/dc/terms/title"] = [{"@value": title}]
    if part_of:
        entity[FRBR_PART_OF] = [{"@id": p} for p in part_of]
    if id_uris:
        entity["http://purl.org/spar/datacite/hasIdentifier"] = [
            {"@id": i} for i in id_uris
        ]
    return entity


def _make_id_entity(uri, scheme_suffix, value):
    return {
        "@id": uri,
        "@type": ["http://purl.org/spar/datacite/Identifier"],
        "http://purl.org/spar/datacite/usesIdentifierScheme": [
            {"@id": f"http://purl.org/spar/datacite/{scheme_suffix}"}
        ],
        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
            {
                "@type": "http://www.w3.org/2001/XMLSchema#string",
                "@value": value,
            }
        ],
    }


@pytest.fixture
def rdf_dir_same_venue(tmp_path):
    """Article → 2 issues → 2 volumes → SAME journal. Classic duplicate."""
    br_dir = tmp_path / "rdf" / "br" / "060" / "10000"
    br_dir.mkdir(parents=True)
    id_dir = tmp_path / "rdf" / "id" / "060" / "10000"
    id_dir.mkdir(parents=True)

    br_data = [
        {
            "@graph": [
                _make_entity(
                    BR1_URI,
                    [EXPRESSION, JOURNAL_ARTICLE],
                    title="Test Article",
                    part_of=[ISSUE_A_URI, ISSUE_B_URI],
                    id_uris=[ID1_URI],
                ),
                _make_entity(
                    ISSUE_A_URI,
                    [EXPRESSION, JOURNAL_ISSUE],
                    part_of=[VOLUME_A_URI],
                ),
                _make_entity(
                    ISSUE_B_URI,
                    [EXPRESSION, JOURNAL_ISSUE],
                    part_of=[VOLUME_B_URI],
                ),
                _make_entity(
                    VOLUME_A_URI,
                    [EXPRESSION, JOURNAL_VOLUME],
                    part_of=[JOURNAL_URI],
                ),
                _make_entity(
                    VOLUME_B_URI,
                    [EXPRESSION, JOURNAL_VOLUME],
                    part_of=[JOURNAL_URI],
                ),
                _make_entity(
                    JOURNAL_URI,
                    [EXPRESSION, JOURNAL_TYPE],
                    title="Test Journal",
                ),
            ]
        }
    ]
    (br_dir / "1000.json").write_bytes(orjson.dumps(br_data))

    id_data = [{"@graph": [_make_id_entity(ID1_URI, "doi", "10.1234/test")]}]
    (id_dir / "1000.json").write_bytes(orjson.dumps(id_data))

    return str(tmp_path / "rdf") + os.sep


@pytest.fixture
def rdf_dir_different_venues(tmp_path):
    """Article → 2 issues → 2 DIFFERENT journals. Conflicting."""
    br_dir = tmp_path / "rdf" / "br" / "060" / "10000"
    br_dir.mkdir(parents=True)
    id_dir = tmp_path / "rdf" / "id" / "060" / "10000"
    id_dir.mkdir(parents=True)

    br_data = [
        {
            "@graph": [
                _make_entity(
                    BR1_URI,
                    [EXPRESSION, JOURNAL_ARTICLE],
                    title="Test Article",
                    part_of=[ISSUE_A_URI, ISSUE_B_URI],
                    id_uris=[ID1_URI],
                ),
                _make_entity(
                    ISSUE_A_URI,
                    [EXPRESSION, JOURNAL_ISSUE],
                    part_of=[JOURNAL_URI],
                ),
                _make_entity(
                    ISSUE_B_URI,
                    [EXPRESSION, JOURNAL_ISSUE],
                    part_of=[JOURNAL_B_URI],
                ),
                _make_entity(
                    JOURNAL_URI,
                    [EXPRESSION, JOURNAL_TYPE],
                    title="Journal Alpha",
                ),
                _make_entity(
                    JOURNAL_B_URI,
                    [EXPRESSION, PROCEEDINGS],
                    title="Proceedings Beta",
                ),
            ]
        }
    ]
    (br_dir / "1000.json").write_bytes(orjson.dumps(br_data))

    id_data = [{"@graph": [_make_id_entity(ID1_URI, "doi", "10.5678/test")]}]
    (id_dir / "1000.json").write_bytes(orjson.dumps(id_data))

    return str(tmp_path / "rdf") + os.sep


@pytest.fixture
def rdf_dir_equivalent_venues(tmp_path):
    """Two top-level books with same title/type but different URIs."""
    br_dir = tmp_path / "rdf" / "br" / "060" / "10000"
    br_dir.mkdir(parents=True)

    br_data = [
        {
            "@graph": [
                _make_entity(
                    BR1_URI,
                    [EXPRESSION, REFERENCE_ENTRY],
                    title="Test Entry",
                    part_of=[ISSUE_A_URI, ISSUE_B_URI],
                ),
                _make_entity(
                    ISSUE_A_URI,
                    [EXPRESSION, REFERENCE_BOOK],
                    title="Encyclopedia",
                ),
                _make_entity(
                    ISSUE_B_URI,
                    [EXPRESSION, REFERENCE_BOOK],
                    title="Encyclopedia",
                ),
            ]
        }
    ]
    (br_dir / "1000.json").write_bytes(orjson.dumps(br_data))

    return str(tmp_path / "rdf") + os.sep


@pytest.fixture
def rdf_dir_single_part_of(tmp_path):
    br_dir = tmp_path / "rdf" / "br" / "060" / "10000"
    br_dir.mkdir(parents=True)

    br_data = [
        {
            "@graph": [
                _make_entity(
                    BR1_URI,
                    [EXPRESSION, JOURNAL_ARTICLE],
                    title="Normal Article",
                    part_of=[ISSUE_A_URI],
                ),
                _make_entity(
                    ISSUE_A_URI,
                    [EXPRESSION, JOURNAL_ISSUE],
                    part_of=[JOURNAL_URI],
                ),
                _make_entity(
                    JOURNAL_URI,
                    [EXPRESSION, JOURNAL_TYPE],
                    title="Normal Journal",
                ),
            ]
        }
    ]
    (br_dir / "1000.json").write_bytes(orjson.dumps(br_data))

    return str(tmp_path / "rdf") + os.sep


@pytest.fixture
def rdf_env(tmp_path, redis_service):
    rdf_dir = str(tmp_path / "rdf") + os.sep

    g_set = GraphSet(BASE_IRI, supplier_prefix=SUPPLIER_PREFIX, wanted_label=False)

    br = g_set.add_br(RESP_AGENT, res=BR1_URI)
    br.create_journal_article()
    br.has_title("Test Article")

    issue_a = g_set.add_br(RESP_AGENT, res=ISSUE_A_URI)
    issue_a.create_issue()

    issue_b = g_set.add_br(RESP_AGENT, res=ISSUE_B_URI)
    issue_b.create_issue()

    journal = g_set.add_br(RESP_AGENT, res=JOURNAL_URI)
    journal.create_journal()
    journal.has_title("Test Journal")

    br.is_part_of(issue_a)
    issue_a.is_part_of(journal)
    issue_b.is_part_of(journal)

    storer = Storer(
        g_set, dir_split=DIR_SPLIT, n_file_item=ITEMS_PER_FILE, zip_output=False
    )
    storer.store_all(rdf_dir, BASE_IRI)
    g_set.commit_changes()

    # Add duplicate partOf directly to JSON
    br_file = os.path.join(rdf_dir, "br", "060", "10000", "1000.json")
    with open(br_file, "rb") as f:
        data = orjson.loads(f.read())
    for graph in data:
        for entity in graph.get("@graph", []):
            if entity["@id"] == BR1_URI:
                entity[FRBR_PART_OF].append({"@id": ISSUE_B_URI})
    with open(br_file, "wb") as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))

    config = {
        "base_iri": BASE_IRI,
        "base_output_dir": str(tmp_path),
        "triplestore_url": "http://127.0.0.1:8805?access-token=qlever_test_token",
        "provenance_triplestore_url": "http://127.0.0.1:8806?access-token=qlever_test_token",
        "dir_split_number": DIR_SPLIT,
        "items_per_file": ITEMS_PER_FILE,
        "zip_output_rdf": False,
        "rdf_files_only": True,
        "redis_host": redis_service["host"],
        "redis_port": redis_service["port"],
        "redis_db": 0,
    }
    config_path = str(tmp_path / "meta_config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return {"config_path": config_path, "rdf_dir": rdf_dir, "tmp_path": str(tmp_path)}


def _load_entities(rdf_dir: str) -> dict[str, dict]:
    entities: dict[str, dict] = {}
    for root, _, files in os.walk(rdf_dir):
        if "prov" in root:
            continue
        for fname in files:
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(root, fname)) as f:
                data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    entities[entity["@id"]] = entity
    return entities


class TestFindFile:
    def test_br_entity(self):
        result = find_rdf_file(BR1_URI, "/data/rdf/", 10000, 1000, False)
        assert result == "/data/rdf/br/060/10000/1000.json"

    def test_id_entity(self):
        result = find_rdf_file(ID1_URI, "/data/rdf/", 10000, 1000, False)
        assert result == "/data/rdf/id/060/10000/1000.json"

    def test_zip_extension(self):
        result = find_rdf_file(BR1_URI, "/data/rdf/", 10000, 1000, True)
        assert result == "/data/rdf/br/060/10000/1000.zip"

    def test_higher_number(self):
        uri = "https://w3id.org/oc/meta/br/0601500"
        result = find_rdf_file(uri, "/data/rdf/", 10000, 1000, False)
        assert result == "/data/rdf/br/060/10000/2000.json"

    def test_invalid_uri(self):
        result = find_rdf_file("not-a-uri", "/data/rdf/", 10000, 1000, False)
        assert result == "/data/rdf/not-a-ur/index.json"


class TestScanBrBatch:
    def test_detects_duplicate_part_of(self, rdf_dir_same_venue):
        br_file = os.path.join(
            rdf_dir_same_venue, "br", "060", "10000", "1000.json"
        )
        results = _scan_br_batch([br_file], zip_output=False)

        assert len(results) == 1
        assert results[0][0] == BR1_URI
        assert set(results[0][1]) == {ISSUE_A_URI, ISSUE_B_URI}

    def test_ignores_single_part_of(self, rdf_dir_single_part_of):
        br_file = os.path.join(
            rdf_dir_single_part_of, "br", "060", "10000", "1000.json"
        )
        results = _scan_br_batch([br_file], zip_output=False)

        assert results == []


class TestScanDuplicatePartOf:
    def test_finds_duplicates(self, rdf_dir_same_venue):
        results = scan_duplicate_part_of(
            rdf_dir_same_venue, zip_output=False, workers=1
        )

        assert len(results) == 1
        assert results[0][0] == BR1_URI

    def test_returns_empty_for_clean_data(self, rdf_dir_single_part_of):
        results = scan_duplicate_part_of(
            rdf_dir_single_part_of, zip_output=False, workers=1
        )

        assert results == []


class TestReadEntity:
    def test_reads_existing_entity(self, rdf_dir_same_venue):
        entity = _read_entity(
            BR1_URI, rdf_dir_same_venue, DIR_SPLIT, ITEMS_PER_FILE, False
        )

        assert entity is not None
        assert entity["@id"] == BR1_URI

    def test_returns_none_for_missing(self, rdf_dir_same_venue):
        entity = _read_entity(
            "https://w3id.org/oc/meta/br/0609999",
            rdf_dir_same_venue,
            DIR_SPLIT,
            ITEMS_PER_FILE,
            False,
        )

        assert entity is None


class TestBuildChainMap:
    def test_builds_chain_to_journal(self, rdf_dir_same_venue):
        chain_map, entity_meta = build_chain_map(
            {ISSUE_A_URI, ISSUE_B_URI},
            rdf_dir_same_venue,
            DIR_SPLIT,
            ITEMS_PER_FILE,
            False,
        )

        assert chain_map[ISSUE_A_URI] == [VOLUME_A_URI]
        assert chain_map[ISSUE_B_URI] == [VOLUME_B_URI]
        assert chain_map[VOLUME_A_URI] == [JOURNAL_URI]
        assert chain_map[VOLUME_B_URI] == [JOURNAL_URI]
        assert JOURNAL_URI not in chain_map  # top venue has no partOf
        assert entity_meta[JOURNAL_URI][0] == "Test Journal"


class TestFollowToVenue:
    def test_follows_to_top(self):
        chain_map = {
            "issue1": ["vol1"],
            "vol1": ["journal1"],
        }
        assert _follow_to_venue("issue1", chain_map) == "journal1"

    def test_stops_at_entity_without_parent(self):
        chain_map = {"issue1": ["journal1"]}
        assert _follow_to_venue("issue1", chain_map) == "journal1"

    def test_handles_unknown_uri(self):
        assert _follow_to_venue("unknown", {}) == "unknown"

    def test_stops_on_multiple_parents(self):
        chain_map = {"issue1": ["vol1", "vol2"]}
        assert _follow_to_venue("issue1", chain_map) == "issue1"


class TestResolveCases:
    def test_same_venue_auto_fix(self):
        chain_map = {
            ISSUE_A_URI: [VOLUME_A_URI],
            ISSUE_B_URI: [VOLUME_B_URI],
            VOLUME_A_URI: [JOURNAL_URI],
            VOLUME_B_URI: [JOURNAL_URI],
        }
        entity_meta = {
            JOURNAL_URI: ("Test Journal", frozenset([EXPRESSION, JOURNAL_TYPE])),
        }
        raw_cases = [(BR1_URI, [ISSUE_A_URI, ISSUE_B_URI])]
        resolved = resolve_cases(raw_cases, chain_map, entity_meta)

        assert len(resolved) == 1
        res = resolved[0]
        assert res.correct_part_of == ISSUE_A_URI
        assert res.to_remove == [ISSUE_B_URI]
        assert res.method == "same_venue"

    def test_equivalent_venues_auto_fix(self):
        chain_map: dict[str, list[str]] = {}
        entity_meta = {
            ISSUE_A_URI: (
                "Encyclopedia",
                frozenset([EXPRESSION, REFERENCE_BOOK]),
            ),
            ISSUE_B_URI: (
                "Encyclopedia",
                frozenset([EXPRESSION, REFERENCE_BOOK]),
            ),
        }
        raw_cases = [(BR1_URI, [ISSUE_A_URI, ISSUE_B_URI])]
        resolved = resolve_cases(raw_cases, chain_map, entity_meta)

        assert len(resolved) == 1
        res = resolved[0]
        assert res.correct_part_of == ISSUE_A_URI
        assert res.to_remove == [ISSUE_B_URI]
        assert res.method == "equivalent_venues"

    def test_different_venues_manual_review(self):
        chain_map = {
            ISSUE_A_URI: [JOURNAL_URI],
            ISSUE_B_URI: [JOURNAL_B_URI],
        }
        entity_meta = {
            JOURNAL_URI: ("Journal Alpha", frozenset([EXPRESSION, JOURNAL_TYPE])),
            JOURNAL_B_URI: (
                "Proceedings Beta",
                frozenset([EXPRESSION, PROCEEDINGS]),
            ),
        }
        raw_cases = [(BR1_URI, [ISSUE_A_URI, ISSUE_B_URI])]
        resolved = resolve_cases(raw_cases, chain_map, entity_meta)

        assert len(resolved) == 1
        res = resolved[0]
        assert res.correct_part_of is None
        assert res.to_remove == []
        assert res.method == "manual_review"


class TestFixBrPartOf:
    def test_removes_duplicate_part_of(self, rdf_env):
        entities = _load_entities(rdf_env["rdf_dir"])
        part_of_before = [
            x["@id"] for x in entities[BR1_URI].get(FRBR_PART_OF, [])
        ]
        assert len(part_of_before) == 2

        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_br_part_of(editor, BR1_URI, ISSUE_A_URI, [ISSUE_B_URI])

        entities = _load_entities(rdf_env["rdf_dir"])
        part_of_after = [
            x["@id"] for x in entities[BR1_URI].get(FRBR_PART_OF, [])
        ]
        assert part_of_after == [ISSUE_A_URI]

    def test_generates_provenance(self, rdf_env):
        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_br_part_of(editor, BR1_URI, ISSUE_A_URI, [ISSUE_B_URI])

        prov_path = os.path.join(
            rdf_env["rdf_dir"], "br", "060", "10000", "1000", "prov", "se.json"
        )
        assert os.path.exists(prov_path)

    def test_scan_returns_empty_after_fix(self, rdf_env):
        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_br_part_of(editor, BR1_URI, ISSUE_A_URI, [ISSUE_B_URI])

        results = scan_duplicate_part_of(
            rdf_env["rdf_dir"], zip_output=False, workers=1
        )

        assert results == []


class TestCheckOrphans:
    def test_detects_orphaned_container(self, rdf_dir_single_part_of):
        orphaned_uri = "https://w3id.org/oc/meta/br/0699"
        orphans = check_orphans(
            [orphaned_uri], rdf_dir_single_part_of, zip_output=False, workers=1
        )

        assert orphans == [orphaned_uri]

    def test_container_with_child_not_orphaned(self, rdf_dir_single_part_of):
        orphans = check_orphans(
            [ISSUE_A_URI], rdf_dir_single_part_of, zip_output=False, workers=1
        )

        assert orphans == []


class TestEnrichManualReview:
    def test_enriches_with_identifiers(self, rdf_dir_different_venues):
        manual_cases = [
            ResolvedCase(
                br_uri=BR1_URI,
                correct_part_of=None,
                to_remove=[],
                method="manual_review",
                reason="test",
            )
        ]
        raw_case_map = {BR1_URI: [ISSUE_A_URI, ISSUE_B_URI]}
        entity_meta = {
            ISSUE_A_URI: ("", frozenset([EXPRESSION, JOURNAL_ISSUE])),
            ISSUE_B_URI: ("", frozenset([EXPRESSION, JOURNAL_ISSUE])),
        }
        enriched = enrich_manual_review(
            manual_cases,
            raw_case_map,
            entity_meta,
            rdf_dir_different_venues,
            DIR_SPLIT,
            ITEMS_PER_FILE,
            False,
        )

        assert len(enriched) == 1
        assert enriched[0]["identifiers"] == ["doi:10.5678/test"]
        assert len(enriched[0]["candidates"]) == 2


class TestProgress:
    def test_load_missing_file(self, tmp_path):
        result = _load_progress(str(tmp_path / "missing.json"))
        assert result == set()

    def test_save_and_load_roundtrip(self, tmp_path):
        path = str(tmp_path / "progress.json")
        completed = {BR1_URI, ISSUE_A_URI}

        _save_progress(path, completed)
        result = _load_progress(path)

        assert result == completed


class TestBuildReport:
    def test_report_structure(self):
        resolved = [
            ResolvedCase(
                br_uri=BR1_URI,
                correct_part_of=ISSUE_A_URI,
                to_remove=[ISSUE_B_URI],
                method="same_venue",
                reason="All chains converge",
            )
        ]
        manual_enriched: list[dict] = []
        orphans = [ISSUE_B_URI]
        report = _build_report(resolved, manual_enriched, orphans)

        assert report["summary"]["total_affected"] == 1
        assert report["summary"]["auto_fixed"] == 1
        assert report["summary"]["manual_review"] == 0
        assert report["summary"]["orphaned_containers"] == 1
        assert report["fixed"][0]["kept_part_of"] == ISSUE_A_URI
        assert report["fixed"][0]["removed_part_of"] == [ISSUE_B_URI]
        assert report["orphaned_containers"] == [ISSUE_B_URI]

    def test_manual_review_report(self):
        resolved = [
            ResolvedCase(
                br_uri=BR1_URI,
                correct_part_of=None,
                to_remove=[],
                method="manual_review",
                reason="Containers differ",
            )
        ]
        manual_enriched = [
            {
                "br_uri": BR1_URI,
                "identifiers": ["doi:10.5678/test"],
                "candidates": [
                    {"uri": ISSUE_A_URI, "title": "", "type": "JournalIssue"},
                    {"uri": ISSUE_B_URI, "title": "", "type": "JournalIssue"},
                ],
                "reason": "Containers differ",
            }
        ]
        report = _build_report(resolved, manual_enriched, [])

        assert report["summary"]["manual_review"] == 1
        assert len(report["manual_review"]) == 1
        assert report["manual_review"][0]["br_uri"] == BR1_URI


class TestEndToEnd:
    def test_same_venue_full_pipeline(self, rdf_dir_same_venue):
        raw_cases = scan_duplicate_part_of(
            rdf_dir_same_venue, zip_output=False, workers=1
        )
        assert len(raw_cases) == 1

        all_containers = {u for _, cs in raw_cases for u in cs}
        chain_map, entity_meta = build_chain_map(
            all_containers, rdf_dir_same_venue, DIR_SPLIT, ITEMS_PER_FILE, False
        )
        resolved = resolve_cases(raw_cases, chain_map, entity_meta)

        assert len(resolved) == 1
        assert resolved[0].method == "same_venue"
        assert resolved[0].correct_part_of is not None

    def test_different_venues_full_pipeline(self, rdf_dir_different_venues):
        raw_cases = scan_duplicate_part_of(
            rdf_dir_different_venues, zip_output=False, workers=1
        )
        all_containers = {u for _, cs in raw_cases for u in cs}
        chain_map, entity_meta = build_chain_map(
            all_containers,
            rdf_dir_different_venues,
            DIR_SPLIT,
            ITEMS_PER_FILE,
            False,
        )
        resolved = resolve_cases(raw_cases, chain_map, entity_meta)

        assert len(resolved) == 1
        assert resolved[0].method == "manual_review"
        assert resolved[0].correct_part_of is None

    def test_equivalent_venues_full_pipeline(self, rdf_dir_equivalent_venues):
        raw_cases = scan_duplicate_part_of(
            rdf_dir_equivalent_venues, zip_output=False, workers=1
        )
        all_containers = {u for _, cs in raw_cases for u in cs}
        chain_map, entity_meta = build_chain_map(
            all_containers,
            rdf_dir_equivalent_venues,
            DIR_SPLIT,
            ITEMS_PER_FILE,
            False,
        )
        resolved = resolve_cases(raw_cases, chain_map, entity_meta)

        assert len(resolved) == 1
        assert resolved[0].method == "equivalent_venues"
        assert resolved[0].correct_part_of is not None
