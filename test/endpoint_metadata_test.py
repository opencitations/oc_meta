# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from pathlib import Path
from unittest.mock import call, patch

import pytest
from rdflib import Graph, Literal, Node, URIRef
from rdflib.namespace import DCTERMS, RDF, XSD

from oc_meta.run.endpoint_metadata import (
    ENDPOINT_PROFILES,
    FORMATS,
    INPUT_FORMAT_PROBES,
    RESULT_FORMAT_PROBES,
    SD,
    SERVICE_QUERY_PROBE,
    SPDX_TURTLE_HEADER,
    SPARQL_11_QUERY_PROBE,
    STATISTIC_QUERIES,
    VOID,
    DatasetMetadata,
    EndpointProfile,
    Partition,
    ScopeMetadata,
    ServiceCapabilities,
    build_service_description,
    build_statistic_query,
    collect_statistics,
    collect_scope_metadata,
    detect_features,
    detect_input_formats,
    detect_result_formats,
    detect_service_capabilities,
    detect_supported_languages,
    input_format_query,
    write_service_description,
)

QUERY_ENDPOINT = "http://127.0.0.1:8890/sparql?token=private"
PUBLIC_ENDPOINT = "https://example.org/sparql"
META_BR_GRAPH = "https://w3id.org/oc/meta/br/"
EXAMPLE_PROPERTY_A = URIRef("http://example.org/p-a")
EXAMPLE_PROPERTY_B = URIRef("http://example.org/p-b")
EXAMPLE_CLASS = URIRef("http://example.org/Class")


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        content_type: str,
        body: dict[str, object],
    ) -> None:
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}
        self.body = body

    def json(self) -> dict[str, object]:
        return self.body


def _sparql_count(value: int) -> dict[str, dict[str, list[dict[str, dict[str, str]]]]]:
    return {"results": {"bindings": [{"value": {"value": str(value)}}]}}


def _scope_metadata(triples: int) -> ScopeMetadata:
    return ScopeMetadata(
        statistics={
            VOID.triples: triples,
            VOID.properties: 2,
            VOID.distinctSubjects: 3,
            VOID.distinctObjects: 4,
            VOID.classes: 1,
        },
        property_partitions=[
            Partition(EXAMPLE_PROPERTY_A, 4),
            Partition(EXAMPLE_PROPERTY_B, 5),
        ],
        class_partitions=[Partition(EXAMPLE_CLASS, 6)],
    )


def _dataset_metadata(with_named_graph: bool) -> DatasetMetadata:
    named_graphs = {META_BR_GRAPH: _scope_metadata(3)} if with_named_graph else {}
    return DatasetMetadata(default_scope=_scope_metadata(7), named_graphs=named_graphs)


def _capabilities() -> ServiceCapabilities:
    return ServiceCapabilities(
        supported_languages=[SD.SPARQL11Query],
        result_formats=[FORMATS.SPARQL_Results_JSON, FORMATS.Turtle],
        input_formats=[FORMATS.Turtle],
        features=[SD.BasicFederatedQuery, SD.UnionDefaultGraph],
    )


def _partition_rows(
    graph: Graph,
    subject: Node,
    partition_predicate: URIRef,
    resource_predicate: URIRef,
    count_predicate: URIRef,
) -> list[tuple[Node, Node]]:
    rows: list[tuple[Node, Node]] = []
    for partition in graph.objects(subject, partition_predicate):
        resource = next(graph.objects(partition, resource_predicate))
        count = next(graph.objects(partition, count_predicate))
        rows.append((resource, count))
    return sorted(rows, key=lambda row: str(row[0]))


def test_endpoint_profiles_are_exact() -> None:
    assert ENDPOINT_PROFILES == {
        "index": EndpointProfile(
            title="OpenCitations Index",
            description="OpenCitations Index data",
            uri_space="https://w3id.org/oc/index/",
            named_graphs=(),
        ),
        "index-provenance": EndpointProfile(
            title="OpenCitations Index provenance",
            description="OpenCitations Index provenance data",
            uri_space="https://w3id.org/oc/index/",
            named_graphs=(),
        ),
        "meta": EndpointProfile(
            title="OpenCitations Meta",
            description="OpenCitations Meta entity data",
            uri_space="https://w3id.org/oc/meta/",
            named_graphs=(
                "https://w3id.org/oc/meta/br/",
                "https://w3id.org/oc/meta/id/",
                "https://w3id.org/oc/meta/ra/",
                "https://w3id.org/oc/meta/ar/",
                "https://w3id.org/oc/meta/re/",
            ),
        ),
        "meta-provenance": EndpointProfile(
            title="OpenCitations Meta provenance",
            description="OpenCitations Meta provenance data",
            uri_space="https://w3id.org/oc/meta/",
            named_graphs=(),
        ),
    }


def test_statistic_queries_are_exact() -> None:
    graph_iri = "https://w3id.org/oc/meta/br/"
    assert [(query.predicate, query.query_template) for query in STATISTIC_QUERIES] == [
        (VOID.triples, "SELECT (COUNT(*) AS ?value) WHERE {{ {pattern} }}"),
        (
            VOID.properties,
            "SELECT (COUNT(DISTINCT ?p) AS ?value) WHERE {{ {pattern} }}",
        ),
        (
            VOID.distinctSubjects,
            "SELECT (COUNT(DISTINCT ?s) AS ?value) WHERE {{ {pattern} }}",
        ),
        (
            VOID.distinctObjects,
            "SELECT (COUNT(DISTINCT ?o) AS ?value) WHERE {{ {pattern} }}",
        ),
        (
            VOID.classes,
            "SELECT (COUNT(DISTINCT ?class) AS ?value) WHERE {{ {pattern} }}",
        ),
    ]
    assert [query.pattern_kind for query in STATISTIC_QUERIES] == [
        "triples",
        "triples",
        "triples",
        "triples",
        "classes",
    ]
    assert [build_statistic_query(query, None) for query in STATISTIC_QUERIES] == [
        "SELECT (COUNT(*) AS ?value) WHERE { ?s ?p ?o }",
        "SELECT (COUNT(DISTINCT ?p) AS ?value) WHERE { ?s ?p ?o }",
        "SELECT (COUNT(DISTINCT ?s) AS ?value) WHERE { ?s ?p ?o }",
        "SELECT (COUNT(DISTINCT ?o) AS ?value) WHERE { ?s ?p ?o }",
        "SELECT (COUNT(DISTINCT ?class) AS ?value) "
        "WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class }",
    ]
    assert build_statistic_query(STATISTIC_QUERIES[0], graph_iri) == (
        "SELECT (COUNT(*) AS ?value) WHERE { "
        "GRAPH <https://w3id.org/oc/meta/br/> { ?s ?p ?o } }"
    )


def test_result_format_probes_are_exact() -> None:
    assert [
        (probe.format_iri, probe.media_type, probe.query)
        for probe in RESULT_FORMAT_PROBES
    ] == [
        (
            FORMATS.SPARQL_Results_JSON,
            "application/sparql-results+json",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS.SPARQL_Results_XML,
            "application/sparql-results+xml",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS.SPARQL_Results_CSV,
            "text/csv",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS.SPARQL_Results_TSV,
            "text/tab-separated-values",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS.Turtle,
            "text/turtle",
            "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS.RDF_XML,
            "application/rdf+xml",
            "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS["N-Triples"],
            "application/n-triples",
            "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS["JSON-LD"],
            "application/ld+json",
            "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS.TriG,
            "application/trig",
            "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1",
        ),
        (
            FORMATS["N-Quads"],
            "application/n-quads",
            "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1",
        ),
    ]


def test_input_format_probes_are_exact() -> None:
    assert [(probe.format_iri, probe.media_type) for probe in INPUT_FORMAT_PROBES] == [
        (FORMATS.Turtle, "text/turtle"),
        (FORMATS["N-Triples"], "application/n-triples"),
        (FORMATS.RDF_XML, "application/rdf+xml"),
        (FORMATS["JSON-LD"], "application/ld+json"),
    ]


def test_collect_statistics_queries_endpoint() -> None:
    responses = [
        _sparql_count(11),
        _sparql_count(5),
        _sparql_count(7),
        _sparql_count(9),
        _sparql_count(3),
    ]
    graph_iri = "https://w3id.org/oc/meta/br/"
    with patch(
        "oc_meta.run.endpoint_metadata.execute_sparql",
        side_effect=responses,
    ) as execute_sparql:
        statistics = collect_statistics(QUERY_ENDPOINT, timeout=42, graph_iri=graph_iri)

    assert statistics == {
        VOID.triples: 11,
        VOID.properties: 5,
        VOID.distinctSubjects: 7,
        VOID.distinctObjects: 9,
        VOID.classes: 3,
    }
    assert execute_sparql.call_args_list == [
        call(QUERY_ENDPOINT, build_statistic_query(query, graph_iri), timeout=42)
        for query in STATISTIC_QUERIES
    ]


def test_collect_scope_metadata_for_named_graph_avoids_distinct_counts() -> None:
    responses = [
        {
            "results": {
                "bindings": [
                    {
                        "resource": {"value": str(EXAMPLE_PROPERTY_A)},
                        "count": {"value": "4"},
                    },
                    {
                        "resource": {"value": str(EXAMPLE_PROPERTY_B)},
                        "count": {"value": "5"},
                    },
                ]
            }
        },
        {
            "results": {
                "bindings": [
                    {
                        "resource": {"value": str(EXAMPLE_CLASS)},
                        "count": {"value": "6"},
                    }
                ]
            }
        },
        _sparql_count(11),
    ]
    with patch(
        "oc_meta.run.endpoint_metadata.execute_sparql",
        side_effect=responses,
    ) as execute_sparql:
        metadata = collect_scope_metadata(
            QUERY_ENDPOINT, timeout=42, graph_iri=META_BR_GRAPH
        )

    assert metadata == ScopeMetadata(
        statistics={
            VOID.triples: 11,
            VOID.properties: 2,
            VOID.classes: 1,
        },
        property_partitions=[
            Partition(EXAMPLE_PROPERTY_A, 4),
            Partition(EXAMPLE_PROPERTY_B, 5),
        ],
        class_partitions=[Partition(EXAMPLE_CLASS, 6)],
    )
    assert execute_sparql.call_args_list == [
        call(
            QUERY_ENDPOINT,
            "SELECT ?resource (COUNT(*) AS ?count) WHERE { "
            "GRAPH <https://w3id.org/oc/meta/br/> { ?s ?resource ?o } "
            "} GROUP BY ?resource",
            timeout=42,
        ),
        call(
            QUERY_ENDPOINT,
            "SELECT ?resource (COUNT(*) AS ?count) WHERE { "
            "GRAPH <https://w3id.org/oc/meta/br/> { "
            "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?resource "
            "} } GROUP BY ?resource",
            timeout=42,
        ),
        call(
            QUERY_ENDPOINT,
            "SELECT (COUNT(*) AS ?value) WHERE { "
            "GRAPH <https://w3id.org/oc/meta/br/> { ?s ?p ?o } }",
            timeout=42,
        ),
    ]


def test_detect_supported_languages_keeps_sparql_11_query() -> None:
    with patch(
        "oc_meta.run.endpoint_metadata.requests.get",
        return_value=FakeResponse(
            200, "application/sparql-results+json;charset=utf-8", {}
        ),
    ) as get:
        languages = detect_supported_languages(QUERY_ENDPOINT, timeout=12)

    assert languages == [SD.SPARQL11Query]
    assert get.call_args_list == [
        call(
            QUERY_ENDPOINT,
            params={"query": SPARQL_11_QUERY_PROBE},
            headers={"Accept": "application/sparql-results+json"},
            timeout=12,
        )
    ]


def test_detect_result_formats_keeps_confirmed_formats() -> None:
    responses = [
        FakeResponse(200, "application/sparql-results+json;charset=utf-8", {}),
        FakeResponse(200, "application/sparql-results+xml", {}),
        FakeResponse(406, "application/json", {}),
        FakeResponse(200, "text/tab-separated-values", {}),
        FakeResponse(200, "text/turtle", {}),
        FakeResponse(200, "text/turtle", {}),
        FakeResponse(500, "application/n-triples", {}),
        FakeResponse(200, "application/json", {}),
        FakeResponse(200, "application/trig", {}),
        FakeResponse(200, "application/n-quads", {}),
    ]
    with patch(
        "oc_meta.run.endpoint_metadata.requests.get",
        side_effect=responses,
    ) as get:
        result_formats = detect_result_formats(QUERY_ENDPOINT, timeout=12)

    assert result_formats == [
        FORMATS.SPARQL_Results_JSON,
        FORMATS.SPARQL_Results_XML,
        FORMATS.SPARQL_Results_TSV,
        FORMATS.Turtle,
        FORMATS.TriG,
        FORMATS["N-Quads"],
    ]
    assert get.call_args_list == [
        call(
            QUERY_ENDPOINT,
            params={"query": probe.query},
            headers={"Accept": probe.media_type},
            timeout=12,
        )
        for probe in RESULT_FORMAT_PROBES
    ]


def test_detect_input_formats_keeps_successful_ask_probes() -> None:
    with patch(
        "oc_meta.run.endpoint_metadata.ask_probe",
        side_effect=[True, False, True, False],
    ) as ask_probe:
        input_formats = detect_input_formats(QUERY_ENDPOINT, timeout=12)

    assert input_formats == [FORMATS.Turtle, FORMATS.RDF_XML]
    assert ask_probe.call_args_list == [
        call(QUERY_ENDPOINT, input_format_query(probe), 12)
        for probe in INPUT_FORMAT_PROBES
    ]


def test_detect_features_uses_successful_probes_and_graph_counts() -> None:
    metadata = DatasetMetadata(
        default_scope=_scope_metadata(7),
        named_graphs={
            "https://w3id.org/oc/meta/br/": _scope_metadata(3),
            "https://w3id.org/oc/meta/id/": _scope_metadata(4),
        },
    )
    with patch("oc_meta.run.endpoint_metadata.ask_probe", return_value=True) as ask:
        features = detect_features(
            QUERY_ENDPOINT,
            "meta",
            metadata,
            input_formats=[FORMATS.Turtle],
            timeout=12,
        )

    assert features == [
        SD.DereferencesURIs,
        SD.BasicFederatedQuery,
        SD.UnionDefaultGraph,
    ]
    assert ask.call_args_list == [
        call(QUERY_ENDPOINT, SERVICE_QUERY_PROBE.format(endpoint=QUERY_ENDPOINT), 12)
    ]


def test_detect_service_capabilities_combines_probe_results() -> None:
    metadata = _dataset_metadata(with_named_graph=False)
    with patch(
        "oc_meta.run.endpoint_metadata.detect_input_formats",
        return_value=[FORMATS.Turtle],
    ) as detect_input:
        with patch(
            "oc_meta.run.endpoint_metadata.detect_supported_languages",
            return_value=[SD.SPARQL11Query],
        ) as detect_languages:
            with patch(
                "oc_meta.run.endpoint_metadata.detect_result_formats",
                return_value=[FORMATS.SPARQL_Results_JSON],
            ) as detect_results:
                with patch(
                    "oc_meta.run.endpoint_metadata.detect_features",
                    return_value=[SD.DereferencesURIs],
                ) as detect_features_mock:
                    capabilities = detect_service_capabilities(
                        QUERY_ENDPOINT, "index", 12, metadata
                    )

    assert capabilities == ServiceCapabilities(
        supported_languages=[SD.SPARQL11Query],
        result_formats=[FORMATS.SPARQL_Results_JSON],
        input_formats=[FORMATS.Turtle],
        features=[SD.DereferencesURIs],
    )
    assert detect_input.call_args_list == [call(QUERY_ENDPOINT, 12)]
    assert detect_languages.call_args_list == [call(QUERY_ENDPOINT, 12)]
    assert detect_results.call_args_list == [call(QUERY_ENDPOINT, 12)]
    assert detect_features_mock.call_args_list == [
        call(QUERY_ENDPOINT, "index", metadata, [FORMATS.Turtle], 12)
    ]


def test_build_service_description_for_meta_returns_parsable_rdf() -> None:
    metadata = _dataset_metadata(with_named_graph=True)
    graph = build_service_description(
        "meta", PUBLIC_ENDPOINT, metadata, _capabilities()
    )

    parsed_graph = Graph()
    parsed_graph.parse(data=graph.serialize(format="turtle"), format="turtle")

    service = next(parsed_graph.subjects(RDF.type, SD.Service))
    dataset = next(parsed_graph.objects(service, SD.defaultDataset))
    named_graph = next(parsed_graph.objects(dataset, SD.namedGraph))
    graph_description = next(parsed_graph.objects(named_graph, SD.graph))

    assert len(parsed_graph) == 49
    assert set(parsed_graph.objects(service, SD.endpoint)) == {URIRef(PUBLIC_ENDPOINT)}
    assert set(parsed_graph.objects(service, SD.supportedLanguage)) == {
        SD.SPARQL11Query
    }
    assert set(parsed_graph.objects(service, SD.resultFormat)) == {
        FORMATS.SPARQL_Results_JSON,
        FORMATS.Turtle,
    }
    assert set(parsed_graph.objects(service, SD.inputFormat)) == {FORMATS.Turtle}
    assert set(parsed_graph.objects(service, SD.feature)) == {
        SD.BasicFederatedQuery,
        SD.UnionDefaultGraph,
    }
    assert set(parsed_graph.objects(dataset, RDF.type)) == {SD.Dataset, VOID.Dataset}
    assert set(parsed_graph.objects(dataset, DCTERMS.title)) == {
        Literal("OpenCitations Meta", lang="en")
    }
    assert set(parsed_graph.objects(dataset, DCTERMS.description)) == {
        Literal("OpenCitations Meta entity data", lang="en")
    }
    assert set(parsed_graph.objects(dataset, VOID.uriSpace)) == {
        Literal("https://w3id.org/oc/meta/")
    }
    assert set(parsed_graph.objects(dataset, VOID.sparqlEndpoint)) == {
        URIRef(PUBLIC_ENDPOINT)
    }
    assert set(parsed_graph.objects(dataset, VOID.triples)) == {
        Literal(7, datatype=XSD.integer)
    }
    assert _partition_rows(
        parsed_graph, dataset, VOID.propertyPartition, VOID.property, VOID.triples
    ) == [
        (EXAMPLE_PROPERTY_A, Literal(4, datatype=XSD.integer)),
        (EXAMPLE_PROPERTY_B, Literal(5, datatype=XSD.integer)),
    ]
    assert _partition_rows(
        parsed_graph, dataset, VOID.classPartition, VOID["class"], VOID.entities
    ) == [(EXAMPLE_CLASS, Literal(6, datatype=XSD.integer))]
    assert set(parsed_graph.objects(named_graph, RDF.type)) == {SD.NamedGraph}
    assert set(parsed_graph.objects(named_graph, SD.name)) == {URIRef(META_BR_GRAPH)}
    assert set(parsed_graph.objects(graph_description, RDF.type)) == {SD.Graph}
    assert set(parsed_graph.objects(graph_description, VOID.uriSpace)) == {
        Literal(META_BR_GRAPH)
    }
    assert set(parsed_graph.objects(graph_description, VOID.triples)) == {
        Literal(3, datatype=XSD.integer)
    }
    assert _partition_rows(
        parsed_graph,
        graph_description,
        VOID.propertyPartition,
        VOID.property,
        VOID.triples,
    ) == [
        (EXAMPLE_PROPERTY_A, Literal(4, datatype=XSD.integer)),
        (EXAMPLE_PROPERTY_B, Literal(5, datatype=XSD.integer)),
    ]
    assert _partition_rows(
        parsed_graph,
        graph_description,
        VOID.classPartition,
        VOID["class"],
        VOID.entities,
    ) == [(EXAMPLE_CLASS, Literal(6, datatype=XSD.integer))]
    assert list(parsed_graph.objects(dataset, SD.defaultGraph)) == []


def test_build_service_description_without_named_graphs() -> None:
    graph = build_service_description(
        "meta-provenance",
        PUBLIC_ENDPOINT,
        _dataset_metadata(with_named_graph=False),
        _capabilities(),
    )

    parsed_graph = Graph()
    parsed_graph.parse(data=graph.serialize(format="turtle"), format="turtle")
    service = next(parsed_graph.subjects(RDF.type, SD.Service))
    dataset = next(parsed_graph.objects(service, SD.defaultDataset))

    assert list(parsed_graph.objects(dataset, SD.namedGraph)) == []
    assert list(parsed_graph.objects(dataset, SD.defaultGraph)) == []


def test_write_service_description_uses_public_endpoint(tmp_path: Path) -> None:
    output = tmp_path / "description.ttl"
    metadata = _dataset_metadata(with_named_graph=False)
    capabilities = _capabilities()
    with patch(
        "oc_meta.run.endpoint_metadata.collect_dataset_metadata",
        return_value=metadata,
    ) as collect_metadata:
        with patch(
            "oc_meta.run.endpoint_metadata.detect_service_capabilities",
            return_value=capabilities,
        ) as detect:
            write_service_description(
                "index",
                QUERY_ENDPOINT,
                PUBLIC_ENDPOINT,
                output,
                timeout=10,
            )

    graph = Graph()
    graph.parse(output, format="turtle")
    service = next(graph.subjects(RDF.type, SD.Service))
    dataset = next(graph.objects(service, SD.defaultDataset))

    assert collect_metadata.call_args_list == [call("index", QUERY_ENDPOINT, 10)]
    assert detect.call_args_list == [call(QUERY_ENDPOINT, "index", 10, metadata)]
    assert output.read_text(encoding="utf-8").startswith(SPDX_TURTLE_HEADER)
    assert set(graph.objects(service, SD.endpoint)) == {URIRef(PUBLIC_ENDPOINT)}
    assert set(graph.objects(dataset, VOID.triples)) == {
        Literal(7, datatype=XSD.integer)
    }
    assert list(graph.objects(dataset, SD.defaultGraph)) == []


def test_write_service_description_does_not_write_after_query_failure(
    tmp_path: Path,
) -> None:
    output = tmp_path / "description.ttl"
    with patch(
        "oc_meta.run.endpoint_metadata.execute_sparql",
        side_effect=RuntimeError("endpoint unavailable"),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            write_service_description(
                "meta",
                QUERY_ENDPOINT,
                PUBLIC_ENDPOINT,
                output,
                timeout=10,
            )

    assert str(exc_info.value) == "endpoint unavailable"
    assert output.exists() is False
