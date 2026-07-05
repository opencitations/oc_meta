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
from oc_meta.run.merge.csv_utils import parse_merged_entities


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


def test_ra_checker_finds_entities_referencing_removed_ra() -> None:
    graph = Dataset(default_union=True)
    responsible_agent = URIRef("https://w3id.org/oc/meta/ra/1")
    agent_role = URIRef("https://w3id.org/oc/meta/ar/2")
    other_entity = URIRef("https://w3id.org/oc/meta/br/1")
    graph.add((agent_role, ra_checker.PRO.isHeldBy, responsible_agent))
    graph.add(
        (
            other_entity,
            URIRef("https://example.org/hasResponsibleAgent"),
            responsible_agent,
        )
    )

    assert [
        str(referencing_entity)
        for referencing_entity in ra_checker.entities_referencing(
            graph, responsible_agent
        )
    ] == [
        "https://w3id.org/oc/meta/ar/2",
        "https://w3id.org/oc/meta/br/1",
    ]


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


def _date_time(value: str) -> Literal:
    return Literal(value, datatype=XSD.dateTime)


def _write_jsonld_zip(path: Path, graph: Dataset) -> None:
    with zipfile.ZipFile(path, "w") as zip_file:
        zip_file.writestr("se.json", graph.serialize(format="json-ld"))
