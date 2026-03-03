import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

import rdflib

from oc_meta.run.migration.extract_subset import (
    extract_subset,
    get_subjects_by_predicate,
    get_subjects_of_class,
    get_triples_for_entity,
)

CLASS_URI = "http://purl.org/spar/fabio/Expression"
PREDICATE_URI = "http://purl.org/dc/terms/title"
ENTITY_A = "http://example.org/a"
ENTITY_B = "http://example.org/b"
GRAPH_URI = "http://example.org/graph"


def _make_client(responses: list[dict[str, object]]) -> MagicMock:
    client = MagicMock()
    client.query = MagicMock(side_effect=responses)
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    return client


def _bindings(rows: list[dict[str, dict[str, str]]]) -> dict[str, object]:
    return {"results": {"bindings": rows}}


class TestGetSubjectsOfClass:
    def test_returns_subject_values(self) -> None:
        client = _make_client([_bindings([{"s": {"value": ENTITY_A}}])])
        result = get_subjects_of_class(client, CLASS_URI, 10)
        assert result == [ENTITY_A]


class TestGetSubjectsByPredicate:
    def test_returns_subject_values(self) -> None:
        client = _make_client([_bindings([{"s": {"value": ENTITY_A}}])])
        result = get_subjects_by_predicate(client, PREDICATE_URI, 10)
        assert result == [ENTITY_A]


class TestGetTriplesForEntity:
    def test_uri_object_with_graph(self) -> None:
        client = _make_client([_bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": ENTITY_B, "type": "uri"},
            "g": {"value": GRAPH_URI},
        }])])
        quads = get_triples_for_entity(client, ENTITY_A, use_graphs=True)
        assert len(quads) == 1
        s, _p, o, g = quads[0]
        assert s == rdflib.URIRef(ENTITY_A)
        assert o == rdflib.URIRef(ENTITY_B)
        assert g == rdflib.URIRef(GRAPH_URI)

    def test_no_graph(self) -> None:
        client = _make_client([_bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "hello", "type": "literal"},
        }])])
        quads = get_triples_for_entity(client, ENTITY_A, use_graphs=False)
        _s, _p, o, g = quads[0]
        assert o == rdflib.Literal("hello")
        assert g is None

    def test_bnode_object(self) -> None:
        client = _make_client([_bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "b0", "type": "bnode"},
        }])])
        quads = get_triples_for_entity(client, ENTITY_A, use_graphs=False)
        assert isinstance(quads[0][2], rdflib.BNode)

    def test_typed_literal(self) -> None:
        client = _make_client([_bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "42", "type": "typed-literal", "datatype": "http://www.w3.org/2001/XMLSchema#integer"},
        }])])
        quads = get_triples_for_entity(client, ENTITY_A, use_graphs=False)
        assert quads[0][2] == rdflib.Literal("42", datatype="http://www.w3.org/2001/XMLSchema#integer")

    def test_lang_literal(self) -> None:
        client = _make_client([_bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "ciao", "type": "literal", "xml:lang": "it"},
        }])])
        quads = get_triples_for_entity(client, ENTITY_A, use_graphs=False)
        assert quads[0][2] == rdflib.Literal("ciao", lang="it")

    def test_unknown_type(self) -> None:
        client = _make_client([_bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": "x", "type": "unknown"},
        }])])
        quads = get_triples_for_entity(client, ENTITY_A, use_graphs=False)
        assert quads[0][2] == rdflib.Literal("x")


def _sparql_responses_for_extract(use_graphs: bool) -> list[dict[str, object]]:
    subjects = _bindings([{"s": {"value": ENTITY_A}}])
    triple: dict[str, dict[str, str]] = {
        "p": {"value": "http://ex.org/p"},
        "o": {"value": "val", "type": "literal"},
    }
    if use_graphs:
        triple["g"] = {"value": GRAPH_URI}
    triples = _bindings([triple])
    return [subjects, triples]


class TestExtractSubset:
    def test_with_class_uri_and_graphs(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nq")
        responses = _sparql_responses_for_extract(use_graphs=True)
        client = _make_client(responses)
        with patch("oc_meta.run.migration.extract_subset.SPARQLClient", return_value=client):
            count, result_path = extract_subset("http://x", 10, output, False, class_uri=CLASS_URI)
        assert count == 1
        assert result_path == output
        assert Path(output).exists()

    def test_with_predicate_no_graphs_no_recurse(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nt")
        responses = _sparql_responses_for_extract(use_graphs=False)
        client = _make_client(responses)
        with patch("oc_meta.run.migration.extract_subset.SPARQLClient", return_value=client):
            count, _path = extract_subset(
                "http://x", 10, output, False,
                predicate_uri=PREDICATE_URI, use_graphs=False, recurse=False,
            )
        assert count == 1
        assert Path(output).exists()

    def test_compress_without_gz_extension(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nq")
        responses = _sparql_responses_for_extract(use_graphs=True)
        client = _make_client(responses)
        with patch("oc_meta.run.migration.extract_subset.SPARQLClient", return_value=client):
            _count, result_path = extract_subset("http://x", 10, output, True, class_uri=CLASS_URI)
        assert result_path == output + ".gz"
        with gzip.open(result_path, "rb") as f:
            ds = rdflib.Dataset()
            ds.parse(data=f.read(), format="nquads")
            assert len(list(ds.quads((None, None, None, None)))) == 1

    def test_compress_with_gz_extension(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nq.gz")
        responses = _sparql_responses_for_extract(use_graphs=True)
        client = _make_client(responses)
        with patch("oc_meta.run.migration.extract_subset.SPARQLClient", return_value=client):
            _count, result_path = extract_subset("http://x", 10, output, True, class_uri=CLASS_URI)
        assert result_path == output

    def test_recurse_follows_uri_objects_and_skips_visited(self, tmp_path: Path) -> None:
        output = str(tmp_path / "out.nt")
        subjects = _bindings([{"s": {"value": ENTITY_A}}])
        triples_a = _bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": ENTITY_B, "type": "uri"},
        }])
        triples_b = _bindings([{
            "p": {"value": "http://ex.org/p"},
            "o": {"value": ENTITY_A, "type": "uri"},
        }])
        client = _make_client([subjects, triples_a, triples_b])
        with patch("oc_meta.run.migration.extract_subset.SPARQLClient", return_value=client):
            count, _ = extract_subset(
                "http://x", 10, output, False,
                class_uri=CLASS_URI, use_graphs=False, recurse=True,
            )
        assert count == 2
