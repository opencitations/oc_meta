# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import gzip
from pathlib import Path
from unittest.mock import patch

import rdflib

from oc_meta.run.migration.extract_subset import (
    extract_subset,
    get_subjects_of_class,
    get_triples_for_entities,
    load_entities_from_file,
    parse_object,
)

CLASS_URI = "http://purl.org/spar/fabio/Expression"
ENTITY_A = "http://example.org/a"
ENTITY_B = "http://example.org/b"
GRAPH_URI = "http://example.org/graph"


def _bindings(rows: list[dict[str, dict[str, str]]]) -> dict[str, object]:
    return {"results": {"bindings": rows}}


class TestGetSubjectsOfClass:
    def test_returns_subject_values(self) -> None:
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", return_value=_bindings([{"s": {"value": ENTITY_A}}])):
            assert get_subjects_of_class("http://x", CLASS_URI, 10) == [ENTITY_A]


class TestLoadEntitiesFromFile:
    def test_reads_uris_skipping_blank_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "entities.txt"
        f.write_text(f"{ENTITY_A}\n\n{ENTITY_B}\n")
        assert load_entities_from_file(str(f)) == [ENTITY_A, ENTITY_B]


class TestParseObject:
    def test_bnode(self) -> None:
        result: dict[str, dict[str, str]] = {"o": {"value": "b0", "type": "bnode"}}
        assert parse_object(result) == rdflib.BNode("b0")

    def test_typed_literal(self) -> None:
        result: dict[str, dict[str, str]] = {"o": {"value": "42", "type": "typed-literal", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}}
        assert parse_object(result) == rdflib.Literal("42", datatype="http://www.w3.org/2001/XMLSchema#integer")

    def test_lang_literal(self) -> None:
        result: dict[str, dict[str, str]] = {"o": {"value": "ciao", "type": "literal", "xml:lang": "it"}}
        assert parse_object(result) == rdflib.Literal("ciao", lang="it")


class TestGetTriplesForEntities:
    def test_with_graphs(self) -> None:
        response = _bindings([{
            "s": {"value": ENTITY_A},
            "p": {"value": "http://ex.org/p"},
            "o": {"value": ENTITY_B, "type": "uri"},
            "g": {"value": GRAPH_URI},
        }])
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", return_value=response):
            quads = get_triples_for_entities("http://x", [ENTITY_A], use_graphs=True)
        assert quads == [(rdflib.URIRef(ENTITY_A), rdflib.URIRef("http://ex.org/p"), rdflib.URIRef(ENTITY_B), rdflib.URIRef(GRAPH_URI))]

    def test_without_graphs(self) -> None:
        response = _bindings([{
            "s": {"value": ENTITY_A},
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "hello", "type": "literal"},
        }])
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", return_value=response):
            quads = get_triples_for_entities("http://x", [ENTITY_A], use_graphs=False)
        assert quads == [(rdflib.URIRef(ENTITY_A), rdflib.URIRef("http://ex.org/p"), rdflib.Literal("hello"), None)]


class TestExtractSubset:
    def test_class_uri_with_graphs_and_compress(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nq")
        responses = [
            _bindings([{"s": {"value": ENTITY_A}}]),
            _bindings([{
                "s": {"value": ENTITY_A},
                "p": {"value": "http://ex.org/p"},
                "o": {"value": "val", "type": "literal"},
                "g": {"value": GRAPH_URI},
            }]),
        ]
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", side_effect=responses):
            count, result_path = extract_subset("http://x", 10, output, True, class_uri=CLASS_URI)
        assert count == 1
        assert result_path == output + ".gz"
        with gzip.open(result_path, "rb") as f:
            assert len(f.read()) > 0

    def test_entities_file_no_graphs(self, tmp_path: Path) -> None:
        entities = tmp_path / "entities.txt"
        entities.write_text(f"{ENTITY_A}\n")
        output = str(tmp_path / "out.nt")
        responses = [_bindings([{
            "s": {"value": ENTITY_A},
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "val", "type": "literal"},
        }])]
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", side_effect=responses):
            count, result_path = extract_subset("http://x", 10, output, False, entities_file=str(entities), use_graphs=False)
        assert count == 1
        assert result_path == output
        assert Path(output).exists()

    def test_compress_with_gz_extension(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nq.gz")
        responses = [
            _bindings([{"s": {"value": ENTITY_A}}]),
            _bindings([{
                "s": {"value": ENTITY_A},
                "p": {"value": "http://ex.org/p"},
                "o": {"value": "val", "type": "literal"},
                "g": {"value": GRAPH_URI},
            }]),
        ]
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", side_effect=responses):
            _count, result_path = extract_subset("http://x", 10, output, True, class_uri=CLASS_URI)
        assert result_path == output

    def test_recurse_follows_uri_objects(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nt")
        responses = [
            _bindings([{"s": {"value": ENTITY_A}}]),
            _bindings([{
                "s": {"value": ENTITY_A},
                "p": {"value": "http://ex.org/p"},
                "o": {"value": ENTITY_B, "type": "uri"},
            }]),
            _bindings([{
                "s": {"value": ENTITY_B},
                "p": {"value": "http://ex.org/p"},
                "o": {"value": ENTITY_A, "type": "uri"},
            }]),
        ]
        with patch("oc_meta.run.migration.extract_subset.execute_sparql", side_effect=responses):
            count, _ = extract_subset("http://x", 10, output, False, class_uri=CLASS_URI, use_graphs=False)
        assert count == 2
