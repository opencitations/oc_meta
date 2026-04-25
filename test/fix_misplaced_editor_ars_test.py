# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import os

import pytest
import yaml
from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet

from oc_meta.core.editor import MetaEditor
from oc_meta.run.patches.fix_misplaced_editor_ars import (
    _load_progress,
    _save_progress,
    find_misplaced_editor_ars,
    fix_chapter,
)

RESP_AGENT = "https://w3id.org/oc/meta/prov/pa/1"
BASE_IRI = "https://w3id.org/oc/meta/"
SUPPLIER_PREFIX = "060"
DIR_SPLIT = 10000
ITEMS_PER_FILE = 1000

CHAPTER_URI = "https://w3id.org/oc/meta/br/0601"
BOOK_URI = "https://w3id.org/oc/meta/br/0602"
AR_URI = "https://w3id.org/oc/meta/ar/0601"

PRO_IS_DOC_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
FRBR_PART_OF = "http://purl.org/vocab/frbr/core#partOf"


@pytest.fixture
def rdf_env(tmp_path, redis_service):
    rdf_dir = str(tmp_path / "rdf") + os.sep

    g_set = GraphSet(BASE_IRI, supplier_prefix=SUPPLIER_PREFIX, wanted_label=False)

    chapter = g_set.add_br(RESP_AGENT, res=CHAPTER_URI)
    chapter.create_book_chapter()

    book = g_set.add_br(RESP_AGENT, res=BOOK_URI)
    book.create_book()

    ar = g_set.add_ar(RESP_AGENT, res=AR_URI)
    ar.create_editor()

    chapter.is_part_of(book)
    chapter.has_contributor(ar)

    storer = Storer(g_set, dir_split=DIR_SPLIT, n_file_item=ITEMS_PER_FILE, zip_output=False)
    storer.store_all(rdf_dir, BASE_IRI)
    g_set.commit_changes()

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


class TestFindMisplacedEditorArs:
    def test_detects_misplaced_ar(self, rdf_env):
        cases = find_misplaced_editor_ars(rdf_env["rdf_dir"], zip_output=False)

        assert len(cases) == 1
        assert cases[0]["chapter"] == CHAPTER_URI
        assert cases[0]["book"] == BOOK_URI
        assert cases[0]["ar"] == AR_URI

    def test_ignores_non_container_editor_types(self, tmp_path):
        rdf_dir = str(tmp_path / "rdf") + os.sep
        g_set = GraphSet(BASE_IRI, supplier_prefix=SUPPLIER_PREFIX, wanted_label=False)

        article = g_set.add_br(RESP_AGENT, res=CHAPTER_URI)
        article.create_journal_article()

        issue = g_set.add_br(RESP_AGENT, res=BOOK_URI)
        issue.create_issue()

        ar = g_set.add_ar(RESP_AGENT, res=AR_URI)
        ar.create_editor()

        article.is_part_of(issue)
        article.has_contributor(ar)

        storer = Storer(g_set, dir_split=DIR_SPLIT, n_file_item=ITEMS_PER_FILE, zip_output=False)
        storer.store_all(rdf_dir, BASE_IRI)
        g_set.commit_changes()

        cases = find_misplaced_editor_ars(rdf_dir, zip_output=False)

        assert cases == []

    def test_returns_empty_after_fix(self, rdf_env):
        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_chapter(editor, CHAPTER_URI, BOOK_URI, [AR_URI])

        cases = find_misplaced_editor_ars(rdf_env["rdf_dir"], zip_output=False)

        assert cases == []


class TestFixChapter:
    def test_ar_moved_from_chapter_to_book(self, rdf_env):
        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_chapter(editor, CHAPTER_URI, BOOK_URI, [AR_URI])

        entities = _load_entities(rdf_env["rdf_dir"])
        chapter_ar_ids = [x["@id"] for x in entities[CHAPTER_URI].get(PRO_IS_DOC_CONTEXT_FOR, [])]
        book_ar_ids = [x["@id"] for x in entities[BOOK_URI].get(PRO_IS_DOC_CONTEXT_FOR, [])]

        assert AR_URI not in chapter_ar_ids
        assert AR_URI in book_ar_ids

    def test_preserves_frbr_part_of(self, rdf_env):
        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_chapter(editor, CHAPTER_URI, BOOK_URI, [AR_URI])

        entities = _load_entities(rdf_env["rdf_dir"])
        part_of_ids = [x["@id"] for x in entities[CHAPTER_URI].get(FRBR_PART_OF, [])]

        assert BOOK_URI in part_of_ids

    def test_generates_provenance_files(self, rdf_env):
        editor = MetaEditor(rdf_env["config_path"], RESP_AGENT)
        fix_chapter(editor, CHAPTER_URI, BOOK_URI, [AR_URI])

        br_se_path = os.path.join(rdf_env["rdf_dir"], "br", "060", "10000", "1000", "prov", "se.json")
        assert os.path.exists(br_se_path)


class TestProgress:
    def test_load_progress_returns_empty_set_when_file_missing(self, tmp_path):
        result = _load_progress(str(tmp_path / "nonexistent.json"))
        assert result == set()

    def test_save_and_load_progress_roundtrip(self, tmp_path):
        path = str(tmp_path / "progress.json")
        completed = {"https://w3id.org/oc/meta/br/0601", "https://w3id.org/oc/meta/br/0602"}

        _save_progress(path, completed)
        result = _load_progress(path)

        assert result == completed
