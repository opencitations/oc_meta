# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import zipfile
from pathlib import Path

from rdflib import RDF, Dataset, Literal, URIRef
from rdflib.namespace import XSD

from oc_meta.run.merge import check_merged_brs_results as br_checker
from oc_meta.run.merge import check_merged_ids_results as id_checker
from oc_meta.run.merge import check_merged_ras_results as ra_checker
from oc_meta.run.merge import check_utils
from oc_meta.run.merge.csv_utils import parse_merged_entities


def _normalize_sparql(query):
    return " ".join(query.split())


def test_parse_merged_entities_accepts_semicolon_with_or_without_space() -> None:
    assert parse_merged_entities("id/1;id/2; id/3 ;  id/4") == [
        "id/1",
        "id/2",
        "id/3",
        "id/4",
    ]


def test_br_checker_reads_ocdm_publication_date_predicate() -> None:
    graph = Dataset(default_union=True)
    entity = URIRef("https://w3id.org/oc/meta/br/1")
    graph.add((entity, RDF.type, URIRef(br_checker.FABIO + "Expression")))
    graph.add(
        (
            entity,
            URIRef(br_checker.DATACITE + "hasIdentifier"),
            URIRef("https://w3id.org/oc/meta/id/1"),
        )
    )
    graph.add((entity, br_checker.PRISM.publicationDate, Literal("2023")))
    graph.add((entity, br_checker.PRISM.publicationDate, Literal("2024")))

    assert br_checker.check_br_constraints(graph, entity) == [
        f"Entity {entity} has multiple publication dates"
    ]


def test_id_provenance_accepts_invalidated_at_time_for_merged_entity(
    tmp_path: Path, monkeypatch
) -> None:
    entity = URIRef("https://w3id.org/oc/meta/id/1")
    first_snapshot = URIRef("https://w3id.org/oc/meta/id/1/prov/se/1")
    second_snapshot = URIRef("https://w3id.org/oc/meta/id/1/prov/se/2")
    graph = Dataset(default_union=True)
    graph.add((first_snapshot, id_checker.PROV.specializationOf, entity))
    graph.add(
        (
            first_snapshot,
            id_checker.PROV.generatedAtTime,
            _date_time("2024-01-01T00:00:00Z"),
        )
    )
    graph.add(
        (
            first_snapshot,
            id_checker.PROV.invalidatedAtTime,
            _date_time("2024-01-02T00:00:00Z"),
        )
    )
    graph.add((second_snapshot, id_checker.PROV.specializationOf, entity))
    graph.add(
        (
            second_snapshot,
            id_checker.PROV.generatedAtTime,
            _date_time("2024-01-02T00:00:00Z"),
        )
    )
    graph.add(
        (
            second_snapshot,
            id_checker.PROV.invalidatedAtTime,
            _date_time("2024-01-03T00:00:00Z"),
        )
    )
    graph.add((second_snapshot, id_checker.PROV.wasDerivedFrom, first_snapshot))
    prov_file_path = tmp_path / "se.zip"
    _write_jsonld_zip(prov_file_path, graph)
    messages = []
    monkeypatch.setattr(id_checker.tqdm, "write", messages.append)

    id_checker.check_provenance(prov_file_path, str(entity), False)

    assert messages == []


def test_id_sparql_check_reports_reference_when_removed_id_is_missing(
    monkeypatch,
) -> None:
    entity = "https://w3id.org/oc/meta/id/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert max_retries == 3
        assert backoff_factor == 1
        if "?p ?o" in query:
            return {"boolean": False}
        if "?s ?p" in query:
            return {"boolean": True}
        raise AssertionError(query)

    messages = []
    monkeypatch.setattr(id_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(id_checker.tqdm, "write", messages.append)

    assert (
        id_checker.check_entity_sparql("https://example.test/sparql", entity, False)
        is True
    )
    assert messages == [
        "Error in SPARQL: Merged entity https://w3id.org/oc/meta/id/1 is still referenced by other entities"
    ]


def test_br_sparql_check_reports_reference_when_removed_br_is_missing(
    monkeypatch,
) -> None:
    entity = "https://w3id.org/oc/meta/br/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert max_retries == 3
        assert backoff_factor == 1
        if "?p ?o" in query:
            return {"boolean": False}
        if "?s ?p" in query:
            return {"boolean": True}
        raise AssertionError(query)

    messages = []
    monkeypatch.setattr(br_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(br_checker.tqdm, "write", messages.append)

    assert (
        br_checker.check_entity_sparql("https://example.test/sparql", entity, False)
        is True
    )
    assert messages == [
        "Error in SPARQL: Merged entity https://w3id.org/oc/meta/br/1 is still referenced by other entities"
    ]


def test_br_checker_flags_duplicate_roles_on_cascaded_containers(monkeypatch) -> None:
    entity = "https://w3id.org/oc/meta/br/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert max_retries == 3
        assert backoff_factor == 1
        expected_query = f"""
    PREFIX pro: <{br_checker.PRO}>
    PREFIX frbr: <{br_checker.FRBR}>
    SELECT ?container ?roleType ?agent (COUNT(DISTINCT ?ar) AS ?count) WHERE {{
        {{ BIND(<{entity}> AS ?container) }}
        UNION
        {{ <{entity}> frbr:partOf+ ?container }}
        ?container pro:isDocumentContextFor ?ar .
        ?ar pro:withRole ?roleType .
        ?ar pro:isHeldBy ?agent .
    }}
    GROUP BY ?container ?roleType ?agent
    HAVING (COUNT(DISTINCT ?ar) > 1)
    """
        assert _normalize_sparql(query) == _normalize_sparql(expected_query)
        return {
            "results": {
                "bindings": [
                    {
                        "container": {
                            "value": "https://w3id.org/oc/meta/br/10",
                            "type": "uri",
                        },
                        "roleType": {
                            "value": "http://purl.org/spar/pro/publisher",
                            "type": "uri",
                        },
                        "agent": {
                            "value": "https://w3id.org/oc/meta/ra/5",
                            "type": "uri",
                        },
                        "count": {"value": "2", "type": "literal"},
                    }
                ]
            }
        }

    messages = []
    monkeypatch.setattr(br_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(br_checker.tqdm, "write", messages.append)

    assert (
        br_checker.check_duplicate_contributor_roles(
            "https://example.test/sparql", entity
        )
        is True
    )
    assert messages == [
        "Error in SPARQL: Entity https://w3id.org/oc/meta/br/10 has 2 duplicate "
        "publisher roles held by https://w3id.org/oc/meta/ra/5"
    ]


def test_br_checker_accepts_container_chain_without_duplicate_roles(
    monkeypatch,
) -> None:
    def execute_sparql(_endpoint, _query, max_retries, backoff_factor):
        return {"results": {"bindings": []}}

    messages = []
    monkeypatch.setattr(br_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(br_checker.tqdm, "write", messages.append)

    assert (
        br_checker.check_duplicate_contributor_roles(
            "https://example.test/sparql", "https://w3id.org/oc/meta/br/1"
        )
        is False
    )
    assert messages == []


def test_br_checker_flags_duplicate_identifiers(monkeypatch) -> None:
    entity = "https://w3id.org/oc/meta/br/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert max_retries == 3
        assert backoff_factor == 1
        expected_query = f"""
    PREFIX datacite: <{br_checker.DATACITE}>
    PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
    SELECT ?scheme ?value (COUNT(DISTINCT ?identifier) AS ?count) WHERE {{
        <{entity}> datacite:hasIdentifier ?identifier .
        ?identifier datacite:usesIdentifierScheme ?scheme .
        ?identifier literal:hasLiteralValue ?value .
    }}
    GROUP BY ?scheme ?value
    HAVING (COUNT(DISTINCT ?identifier) > 1)
    """
        assert _normalize_sparql(query) == _normalize_sparql(expected_query)
        return {
            "results": {
                "bindings": [
                    {
                        "scheme": {
                            "value": "http://purl.org/spar/datacite/doi",
                            "type": "uri",
                        },
                        "value": {"value": "10.1234/example", "type": "literal"},
                        "count": {"value": "2", "type": "literal"},
                    }
                ]
            }
        }

    messages = []
    monkeypatch.setattr(br_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(br_checker.tqdm, "write", messages.append)

    assert (
        br_checker.check_duplicate_identifiers("https://example.test/sparql", entity)
        is True
    )
    assert messages == [
        "Error in SPARQL: Entity https://w3id.org/oc/meta/br/1 has 2 "
        "duplicate identifiers with scheme doi and value 10.1234/example"
    ]


def test_br_checker_accepts_unique_identifiers(monkeypatch) -> None:
    def execute_sparql(_endpoint, _query, max_retries, backoff_factor):
        return {"results": {"bindings": []}}

    messages = []
    monkeypatch.setattr(br_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(br_checker.tqdm, "write", messages.append)

    assert (
        br_checker.check_duplicate_identifiers(
            "https://example.test/sparql", "https://w3id.org/oc/meta/br/1"
        )
        is False
    )
    assert messages == []


def test_ra_sparql_check_reports_reference_for_removed_ra(
    monkeypatch,
) -> None:
    entity = "https://w3id.org/oc/meta/ra/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert max_retries == 3
        assert backoff_factor == 1
        if "?p ?o" in query:
            return {"boolean": False}
        if "?s ?p" in query and "pro/isHeldBy" not in query:
            return {"boolean": True}
        raise AssertionError(query)

    messages = []
    monkeypatch.setattr(ra_checker, "execute_sparql", execute_sparql)
    monkeypatch.setattr(ra_checker.tqdm, "write", messages.append)

    assert (
        ra_checker.check_entity_sparql("https://example.test/sparql", entity, False)
        is True
    )
    assert messages == [
        "Error in SPARQL: Merged responsible agent https://w3id.org/oc/meta/ra/1 is still referenced by other entities"
    ]


def test_id_checker_reads_all_zip_members(tmp_path: Path, monkeypatch) -> None:
    entity = URIRef("https://w3id.org/oc/meta/id/1")
    unrelated_graph = Dataset(default_union=True)
    unrelated_graph.add(
        (
            URIRef("https://w3id.org/oc/meta/id/2"),
            RDF.type,
            URIRef(id_checker.DATACITE + "Identifier"),
        )
    )
    entity_graph = Dataset(default_union=True)
    entity_graph.add((entity, RDF.type, URIRef(id_checker.DATACITE + "Identifier")))
    entity_graph.add(
        (
            entity,
            URIRef(id_checker.DATACITE + "usesIdentifierScheme"),
            URIRef(id_checker.DATACITE + "doi"),
        )
    )
    entity_graph.add(
        (
            entity,
            URIRef(id_checker.LITERAL_REIFICATION + "hasLiteralValue"),
            Literal("10.1234/example"),
        )
    )
    file_path = tmp_path / "1000.zip"
    with zipfile.ZipFile(file_path, "w") as zip_file:
        zip_file.writestr("part1.json", unrelated_graph.serialize(format="json-ld"))
        zip_file.writestr("part2.json", entity_graph.serialize(format="json-ld"))
    _write_survivor_provenance_zip(tmp_path, entity)

    messages = []
    monkeypatch.setattr(id_checker.tqdm, "write", messages.append)

    id_checker.check_entity_file(str(file_path), str(entity), True)

    assert messages == []


def test_id_checker_reports_missing_file(monkeypatch) -> None:
    messages = []
    monkeypatch.setattr(id_checker.tqdm, "write", messages.append)

    id_checker.check_entity_file(
        "does/not/exist.zip", "https://w3id.org/oc/meta/id/1", True
    )

    assert messages == [
        "Error: File not found for entity https://w3id.org/oc/meta/id/1"
    ]


def test_id_checker_deduplicates_triples_across_contexts() -> None:
    entity = URIRef("https://w3id.org/oc/meta/id/1")
    graph = Dataset(default_union=True)
    first_context = graph.graph(URIRef("https://w3id.org/oc/meta/id/"))
    second_context = graph.graph(URIRef("https://example.org/other"))
    for context in (first_context, second_context):
        context.add((entity, RDF.type, URIRef(id_checker.DATACITE + "Identifier")))
        context.add(
            (
                entity,
                URIRef(id_checker.DATACITE + "usesIdentifierScheme"),
                URIRef(id_checker.DATACITE + "doi"),
            )
        )
        context.add(
            (
                entity,
                URIRef(id_checker.LITERAL_REIFICATION + "hasLiteralValue"),
                Literal("10.1234/example"),
            )
        )

    assert id_checker.check_identifier_constraints(graph, entity) == []


def test_id_checker_flags_entity_that_is_not_an_identifier() -> None:
    entity = URIRef("https://w3id.org/oc/meta/id/1")
    graph = Dataset(default_union=True)
    graph.add((entity, RDF.type, URIRef("http://purl.org/spar/fabio/Expression")))

    assert id_checker.check_identifier_constraints(graph, entity) == [
        f"Entity {entity} is not a datacite:Identifier",
        f"Entity {entity} should have exactly one usesIdentifierScheme, found 0",
        f"Entity {entity} should have exactly one hasLiteralValue, found 0",
    ]


def _has_next_execute_sparql(cycle=(), forks=(), shared=(), heads=()):
    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert max_retries == 3
        assert backoff_factor == 1
        if "oco:hasNext+" in query:
            return {
                "results": {
                    "bindings": [{"ar": {"value": ar, "type": "uri"}} for ar in cycle]
                }
            }
        if "GROUP BY ?ar" in query:
            return {
                "results": {
                    "bindings": [
                        {
                            "ar": {"value": ar, "type": "uri"},
                            "count": {"value": str(count), "type": "literal"},
                        }
                        for ar, count in forks
                    ]
                }
            }
        if "GROUP BY ?next" in query:
            return {
                "results": {
                    "bindings": [
                        {
                            "next": {"value": ar, "type": "uri"},
                            "count": {"value": str(count), "type": "literal"},
                        }
                        for ar, count in shared
                    ]
                }
            }
        if "GROUP BY ?br ?roleType" in query:
            return {
                "results": {
                    "bindings": [
                        {
                            "br": {"value": br, "type": "uri"},
                            "roleType": {"value": role_type, "type": "uri"},
                            "count": {"value": str(count), "type": "literal"},
                        }
                        for br, role_type, count in heads
                    ]
                }
            }
        raise AssertionError(query)

    return execute_sparql


def test_has_next_chain_issues_flags_cycle_fork_shared_next_and_heads(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        check_utils,
        "execute_sparql",
        _has_next_execute_sparql(
            cycle=["https://w3id.org/oc/meta/ar/1"],
            forks=[("https://w3id.org/oc/meta/ar/2", 2)],
            shared=[("https://w3id.org/oc/meta/ar/3", 2)],
            heads=[
                (
                    "https://w3id.org/oc/meta/br/1",
                    "http://purl.org/spar/pro/author",
                    2,
                )
            ],
        ),
    )

    assert check_utils.has_next_chain_issues(
        "https://example.test/sparql", "BIND(<https://w3id.org/oc/meta/br/1> AS ?br)"
    ) == [
        "Agent role https://w3id.org/oc/meta/ar/1 is part of an oco:hasNext cycle",
        "Agent role https://w3id.org/oc/meta/ar/2 has 2 oco:hasNext successors",
        "Agent role https://w3id.org/oc/meta/ar/3 has 2 oco:hasNext predecessors",
        "Bibliographic resource https://w3id.org/oc/meta/br/1 has 2 disconnected author chains",
    ]


def test_br_checker_reports_has_next_cycle_for_survivor(monkeypatch) -> None:
    entity = "https://w3id.org/oc/meta/br/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert f"BIND(<{entity}> AS ?br)" in query
        return _has_next_execute_sparql(cycle=["https://w3id.org/oc/meta/ar/1"])(
            _endpoint, query, max_retries, backoff_factor
        )

    messages = []
    monkeypatch.setattr(check_utils, "execute_sparql", execute_sparql)
    monkeypatch.setattr(br_checker.tqdm, "write", messages.append)

    assert (
        br_checker.check_has_next_integrity("https://example.test/sparql", entity)
        is True
    )
    assert messages == [
        "Error in SPARQL: Agent role https://w3id.org/oc/meta/ar/1 is part of an oco:hasNext cycle"
    ]


def test_ra_checker_reports_has_next_cycle_for_survivor(monkeypatch) -> None:
    entity = "https://w3id.org/oc/meta/ra/1"

    def execute_sparql(_endpoint, query, max_retries, backoff_factor):
        assert f"?held_role pro:isHeldBy <{entity}>" in query
        return _has_next_execute_sparql(cycle=["https://w3id.org/oc/meta/ar/1"])(
            _endpoint, query, max_retries, backoff_factor
        )

    messages = []
    monkeypatch.setattr(check_utils, "execute_sparql", execute_sparql)
    monkeypatch.setattr(ra_checker.tqdm, "write", messages.append)

    assert (
        ra_checker.check_has_next_integrity("https://example.test/sparql", entity)
        is True
    )
    assert messages == [
        "Error in SPARQL: Agent role https://w3id.org/oc/meta/ar/1 is part of an oco:hasNext cycle"
    ]


def _date_time(value: str) -> Literal:
    return Literal(value, datatype=XSD.dateTime)


def _write_jsonld_zip(path: Path, graph: Dataset) -> None:
    with zipfile.ZipFile(path, "w") as zip_file:
        zip_file.writestr("se.json", graph.serialize(format="json-ld"))


def _write_survivor_provenance_zip(tmp_path: Path, entity: URIRef) -> None:
    first_snapshot = URIRef(f"{entity}/prov/se/1")
    second_snapshot = URIRef(f"{entity}/prov/se/2")
    graph = Dataset(default_union=True)
    graph.add((first_snapshot, id_checker.PROV.specializationOf, entity))
    graph.add(
        (
            first_snapshot,
            id_checker.PROV.generatedAtTime,
            _date_time("2024-01-01T00:00:00Z"),
        )
    )
    graph.add(
        (
            first_snapshot,
            id_checker.PROV.invalidatedAtTime,
            _date_time("2024-01-02T00:00:00Z"),
        )
    )
    graph.add((second_snapshot, id_checker.PROV.specializationOf, entity))
    graph.add(
        (
            second_snapshot,
            id_checker.PROV.generatedAtTime,
            _date_time("2024-01-02T00:00:00Z"),
        )
    )
    graph.add((second_snapshot, id_checker.PROV.wasDerivedFrom, first_snapshot))
    prov_dir = tmp_path / "1000" / "prov"
    prov_dir.mkdir(parents=True)
    _write_jsonld_zip(prov_dir / "se.zip", graph)
