# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from urllib.parse import quote

import requests
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, XSD
from rich_argparse import RichHelpFormatter

from oc_meta.lib.sparql import execute_sparql

SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
VOID = Namespace("http://rdfs.org/ns/void#")
FORMATS = Namespace("http://www.w3.org/ns/formats/")

SparqlSelectResult = dict[str, dict[str, list[dict[str, dict[str, str]]]]]
SPDX_TURTLE_HEADER = (
    "# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>\n"
    "#\n"
    "# SPDX-License-"
    "Identifier: ISC\n\n"
)


@dataclass(frozen=True)
class EndpointProfile:
    title: str
    description: str
    uri_space: str
    named_graphs: tuple[str, ...]


@dataclass(frozen=True)
class StatisticQuery:
    predicate: URIRef
    query_template: str
    pattern_kind: str


@dataclass(frozen=True)
class ResultFormatProbe:
    format_iri: URIRef
    media_type: str
    query: str


@dataclass(frozen=True)
class InputFormatProbe:
    format_iri: URIRef
    media_type: str
    payload: str


@dataclass(frozen=True)
class Partition:
    resource: URIRef
    count: int


@dataclass(frozen=True)
class ScopeMetadata:
    statistics: dict[URIRef, int]
    property_partitions: list[Partition]
    class_partitions: list[Partition]


@dataclass(frozen=True)
class DatasetMetadata:
    default_scope: ScopeMetadata
    named_graphs: dict[str, ScopeMetadata]


@dataclass(frozen=True)
class ServiceCapabilities:
    supported_languages: list[URIRef]
    result_formats: list[URIRef]
    input_formats: list[URIRef]
    features: list[URIRef]


ENDPOINT_PROFILES = {
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

STATISTIC_QUERIES = (
    StatisticQuery(
        predicate=VOID.triples,
        query_template="SELECT (COUNT(*) AS ?value) WHERE {{ {pattern} }}",
        pattern_kind="triples",
    ),
    StatisticQuery(
        predicate=VOID.properties,
        query_template=("SELECT (COUNT(DISTINCT ?p) AS ?value) WHERE {{ {pattern} }}"),
        pattern_kind="triples",
    ),
    StatisticQuery(
        predicate=VOID.distinctSubjects,
        query_template=("SELECT (COUNT(DISTINCT ?s) AS ?value) WHERE {{ {pattern} }}"),
        pattern_kind="triples",
    ),
    StatisticQuery(
        predicate=VOID.distinctObjects,
        query_template=("SELECT (COUNT(DISTINCT ?o) AS ?value) WHERE {{ {pattern} }}"),
        pattern_kind="triples",
    ),
    StatisticQuery(
        predicate=VOID.classes,
        query_template="SELECT (COUNT(DISTINCT ?class) AS ?value) WHERE {{ {pattern} }}",
        pattern_kind="classes",
    ),
)

SELECT_QUERY = "SELECT * WHERE { ?s ?p ?o } LIMIT 1"
CONSTRUCT_QUERY = "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 1"
SPARQL_11_QUERY_PROBE = "SELECT * WHERE { VALUES ?s { <urn:oc-meta-probe:s> } } LIMIT 1"
SERVICE_QUERY_PROBE = "ASK {{ SERVICE <{endpoint}> {{ ?s ?p ?o }} }}"

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

PROPERTY_PARTITION_QUERY_TEMPLATE = (
    "SELECT ?resource (COUNT(*) AS ?count) WHERE {{ {pattern} }} GROUP BY ?resource"
)
CLASS_PARTITION_QUERY_TEMPLATE = (
    "SELECT ?resource (COUNT(*) AS ?count) WHERE {{ {pattern} }} GROUP BY ?resource"
)

RESULT_FORMAT_PROBES = (
    ResultFormatProbe(
        format_iri=FORMATS.SPARQL_Results_JSON,
        media_type="application/sparql-results+json",
        query=SELECT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS.SPARQL_Results_XML,
        media_type="application/sparql-results+xml",
        query=SELECT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS.SPARQL_Results_CSV,
        media_type="text/csv",
        query=SELECT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS.SPARQL_Results_TSV,
        media_type="text/tab-separated-values",
        query=SELECT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS.Turtle,
        media_type="text/turtle",
        query=CONSTRUCT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS.RDF_XML,
        media_type="application/rdf+xml",
        query=CONSTRUCT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS["N-Triples"],
        media_type="application/n-triples",
        query=CONSTRUCT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS["JSON-LD"],
        media_type="application/ld+json",
        query=CONSTRUCT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS.TriG,
        media_type="application/trig",
        query=CONSTRUCT_QUERY,
    ),
    ResultFormatProbe(
        format_iri=FORMATS["N-Quads"],
        media_type="application/n-quads",
        query=CONSTRUCT_QUERY,
    ),
)

INPUT_FORMAT_PROBES = (
    InputFormatProbe(
        format_iri=FORMATS.Turtle,
        media_type="text/turtle",
        payload="<urn:oc-meta-probe:s> <urn:oc-meta-probe:p> <urn:oc-meta-probe:o> .",
    ),
    InputFormatProbe(
        format_iri=FORMATS["N-Triples"],
        media_type="application/n-triples",
        payload="<urn:oc-meta-probe:s> <urn:oc-meta-probe:p> <urn:oc-meta-probe:o> .",
    ),
    InputFormatProbe(
        format_iri=FORMATS.RDF_XML,
        media_type="application/rdf+xml",
        payload=(
            '<?xml version="1.0"?>'
            '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" '
            'xmlns:probe="urn:oc-meta-probe:">'
            '<rdf:Description rdf:about="urn:oc-meta-probe:s">'
            '<probe:p rdf:resource="urn:oc-meta-probe:o"/>'
            "</rdf:Description>"
            "</rdf:RDF>"
        ),
    ),
    InputFormatProbe(
        format_iri=FORMATS["JSON-LD"],
        media_type="application/ld+json",
        payload=(
            '[{"@id":"urn:oc-meta-probe:s",'
            '"urn:oc-meta-probe:p":{"@id":"urn:oc-meta-probe:o"}}]'
        ),
    ),
)


def triple_pattern(graph_iri: str | None) -> str:
    if graph_iri is None:
        return "?s ?p ?o"
    return f"GRAPH <{graph_iri}> {{ ?s ?p ?o }}"


def class_pattern(graph_iri: str | None) -> str:
    if graph_iri is None:
        return f"?s <{RDF_TYPE}> ?class"
    return f"GRAPH <{graph_iri}> {{ ?s <{RDF_TYPE}> ?class }}"


def property_partition_pattern(graph_iri: str | None) -> str:
    if graph_iri is None:
        return "?s ?resource ?o"
    return f"GRAPH <{graph_iri}> {{ ?s ?resource ?o }}"


def class_partition_pattern(graph_iri: str | None) -> str:
    if graph_iri is None:
        return f"?s <{RDF_TYPE}> ?resource"
    return f"GRAPH <{graph_iri}> {{ ?s <{RDF_TYPE}> ?resource }}"


def build_statistic_query(statistic: StatisticQuery, graph_iri: str | None) -> str:
    if statistic.pattern_kind == "classes":
        pattern = class_pattern(graph_iri)
    else:
        pattern = triple_pattern(graph_iri)
    return statistic.query_template.format(pattern=pattern)


def collect_statistics(
    endpoint: str, timeout: int, graph_iri: str | None = None
) -> dict[URIRef, int]:
    statistics: dict[URIRef, int] = {}
    for statistic in STATISTIC_QUERIES:
        query = build_statistic_query(statistic, graph_iri)
        result = cast(
            SparqlSelectResult,
            execute_sparql(endpoint, query, timeout=timeout),
        )
        value = result["results"]["bindings"][0]["value"]["value"]
        statistics[statistic.predicate] = int(value)
    return statistics


def collect_statistic(
    endpoint: str,
    timeout: int,
    statistic: StatisticQuery,
    graph_iri: str | None = None,
) -> int:
    result = cast(
        SparqlSelectResult,
        execute_sparql(
            endpoint,
            build_statistic_query(statistic, graph_iri),
            timeout=timeout,
        ),
    )
    return int(result["results"]["bindings"][0]["value"]["value"])


def collect_partitions(
    endpoint: str,
    query: str,
    timeout: int,
) -> list[Partition]:
    result = cast(SparqlSelectResult, execute_sparql(endpoint, query, timeout=timeout))
    partitions: list[Partition] = []
    for binding in result["results"]["bindings"]:
        partitions.append(
            Partition(
                resource=URIRef(binding["resource"]["value"]),
                count=int(binding["count"]["value"]),
            )
        )
    return partitions


def collect_scope_metadata(
    endpoint: str,
    timeout: int,
    graph_iri: str | None = None,
) -> ScopeMetadata:
    property_partitions = collect_partitions(
        endpoint,
        PROPERTY_PARTITION_QUERY_TEMPLATE.format(
            pattern=property_partition_pattern(graph_iri)
        ),
        timeout,
    )
    class_partitions = collect_partitions(
        endpoint,
        CLASS_PARTITION_QUERY_TEMPLATE.format(
            pattern=class_partition_pattern(graph_iri)
        ),
        timeout,
    )
    if graph_iri is None:
        statistics = collect_statistics(endpoint, timeout)
    else:
        statistics = {
            VOID.triples: collect_statistic(
                endpoint, timeout, STATISTIC_QUERIES[0], graph_iri
            ),
            VOID.properties: len(property_partitions),
            VOID.classes: len(class_partitions),
        }
    return ScopeMetadata(
        statistics=statistics,
        property_partitions=property_partitions,
        class_partitions=class_partitions,
    )


def collect_dataset_metadata(
    dataset_name: str,
    endpoint: str,
    timeout: int,
) -> DatasetMetadata:
    profile = ENDPOINT_PROFILES[dataset_name]
    named_graphs: dict[str, ScopeMetadata] = {}
    for graph_iri in profile.named_graphs:
        named_graphs[graph_iri] = collect_scope_metadata(endpoint, timeout, graph_iri)
    return DatasetMetadata(
        default_scope=collect_scope_metadata(endpoint, timeout),
        named_graphs=named_graphs,
    )


def media_type(response: requests.Response) -> str:
    return response.headers["Content-Type"].split(";", 1)[0].strip()


def json_probe(endpoint: str, query: str, timeout: int) -> requests.Response | None:
    try:
        return requests.get(
            endpoint,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=timeout,
        )
    except requests.RequestException:
        return None


def ask_probe(endpoint: str, query: str, timeout: int) -> bool:
    response = json_probe(endpoint, query, timeout)
    if response is None:
        return False
    if not 200 <= response.status_code < 300:
        return False
    if "Content-Type" not in response.headers:
        return False
    if media_type(response) != "application/sparql-results+json":
        return False
    result = cast(dict[str, object], response.json())
    return result["boolean"] is True


def detect_supported_languages(endpoint: str, timeout: int) -> list[URIRef]:
    response = json_probe(endpoint, SPARQL_11_QUERY_PROBE, timeout)
    if response is None:
        return []
    if not 200 <= response.status_code < 300:
        return []
    if "Content-Type" not in response.headers:
        return []
    if media_type(response) != "application/sparql-results+json":
        return []
    return [SD.SPARQL11Query]


def detect_result_formats(endpoint: str, timeout: int) -> list[URIRef]:
    result_formats: list[URIRef] = []
    for probe in RESULT_FORMAT_PROBES:
        try:
            response = requests.get(
                endpoint,
                params={"query": probe.query},
                headers={"Accept": probe.media_type},
                timeout=timeout,
            )
        except requests.RequestException:
            continue
        if "Content-Type" not in response.headers:
            continue
        if (
            200 <= response.status_code < 300
            and media_type(response) == probe.media_type
        ):
            result_formats.append(probe.format_iri)
    return result_formats


def input_format_query(probe: InputFormatProbe) -> str:
    data_iri = f"data:{probe.media_type},{quote(probe.payload, safe='')}"
    return (
        f"ASK FROM <{data_iri}> "
        "WHERE { <urn:oc-meta-probe:s> <urn:oc-meta-probe:p> <urn:oc-meta-probe:o> }"
    )


def detect_input_formats(endpoint: str, timeout: int) -> list[URIRef]:
    input_formats: list[URIRef] = []
    for probe in INPUT_FORMAT_PROBES:
        if ask_probe(endpoint, input_format_query(probe), timeout):
            input_formats.append(probe.format_iri)
    return input_formats


def detect_features(
    endpoint: str,
    dataset_name: str,
    metadata: DatasetMetadata,
    input_formats: list[URIRef],
    timeout: int,
) -> list[URIRef]:
    features: list[URIRef] = []
    if input_formats:
        features.append(SD.DereferencesURIs)
    if ask_probe(endpoint, SERVICE_QUERY_PROBE.format(endpoint=endpoint), timeout):
        features.append(SD.BasicFederatedQuery)
    if dataset_name == "meta":
        named_graph_triples = sum(
            scope.statistics[VOID.triples] for scope in metadata.named_graphs.values()
        )
        if named_graph_triples == metadata.default_scope.statistics[VOID.triples]:
            features.append(SD.UnionDefaultGraph)
    return features


def detect_service_capabilities(
    endpoint: str,
    dataset_name: str,
    timeout: int,
    metadata: DatasetMetadata,
) -> ServiceCapabilities:
    input_formats = detect_input_formats(endpoint, timeout)
    return ServiceCapabilities(
        supported_languages=detect_supported_languages(endpoint, timeout),
        result_formats=detect_result_formats(endpoint, timeout),
        input_formats=input_formats,
        features=detect_features(
            endpoint,
            dataset_name,
            metadata,
            input_formats,
            timeout,
        ),
    )


def add_scope_metadata(
    graph: Graph,
    subject: BNode,
    metadata: ScopeMetadata,
) -> None:
    for predicate, value in metadata.statistics.items():
        graph.add((subject, predicate, Literal(value, datatype=XSD.integer)))
    for partition in metadata.property_partitions:
        partition_node = BNode()
        graph.add((subject, VOID.propertyPartition, partition_node))
        graph.add((partition_node, VOID.property, partition.resource))
        graph.add(
            (
                partition_node,
                VOID.triples,
                Literal(partition.count, datatype=XSD.integer),
            )
        )
    for partition in metadata.class_partitions:
        partition_node = BNode()
        graph.add((subject, VOID.classPartition, partition_node))
        graph.add((partition_node, VOID["class"], partition.resource))
        graph.add(
            (
                partition_node,
                VOID.entities,
                Literal(partition.count, datatype=XSD.integer),
            )
        )


def build_service_description(
    dataset_name: str,
    public_endpoint: str,
    metadata: DatasetMetadata,
    capabilities: ServiceCapabilities,
) -> Graph:
    profile = ENDPOINT_PROFILES[dataset_name]
    service = BNode()
    dataset = BNode()

    graph = Graph()
    graph.bind("sd", SD)
    graph.bind("void", VOID)
    graph.bind("dcterms", DCTERMS)
    graph.bind("formats", FORMATS)

    graph.add((service, RDF.type, SD.Service))
    graph.add((service, SD.endpoint, URIRef(public_endpoint)))
    graph.add((service, SD.defaultDataset, dataset))
    for supported_language in capabilities.supported_languages:
        graph.add((service, SD.supportedLanguage, supported_language))
    for result_format in capabilities.result_formats:
        graph.add((service, SD.resultFormat, result_format))
    for input_format in capabilities.input_formats:
        graph.add((service, SD.inputFormat, input_format))
    for feature in capabilities.features:
        graph.add((service, SD.feature, feature))

    graph.add((dataset, RDF.type, SD.Dataset))
    graph.add((dataset, RDF.type, VOID.Dataset))
    graph.add((dataset, DCTERMS.title, Literal(profile.title, lang="en")))
    graph.add((dataset, DCTERMS.description, Literal(profile.description, lang="en")))
    graph.add((dataset, VOID.uriSpace, Literal(profile.uri_space)))
    graph.add((dataset, VOID.sparqlEndpoint, URIRef(public_endpoint)))

    add_scope_metadata(graph, dataset, metadata.default_scope)
    for graph_iri, graph_metadata in metadata.named_graphs.items():
        named_graph = BNode()
        graph_description = BNode()
        graph.add((dataset, SD.namedGraph, named_graph))
        graph.add((named_graph, RDF.type, SD.NamedGraph))
        graph.add((named_graph, SD.name, URIRef(graph_iri)))
        graph.add((named_graph, SD.graph, graph_description))
        graph.add((graph_description, RDF.type, SD.Graph))
        graph.add((graph_description, VOID.uriSpace, Literal(graph_iri)))
        add_scope_metadata(graph, graph_description, graph_metadata)

    return graph


def write_service_description(
    dataset_name: str,
    endpoint: str,
    public_endpoint: str,
    output: Path,
    timeout: int,
) -> None:
    metadata = collect_dataset_metadata(dataset_name, endpoint, timeout)
    capabilities = detect_service_capabilities(
        endpoint, dataset_name, timeout, metadata
    )
    graph = build_service_description(
        dataset_name, public_endpoint, metadata, capabilities
    )
    turtle = graph.serialize(format="turtle")
    output.write_text(f"{SPDX_TURTLE_HEADER}{turtle}", encoding="utf-8")


def parse_args() -> argparse.Namespace:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Generate SPARQL Service Description RDF for an OpenCitations endpoint.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "dataset",
        choices=sorted(ENDPOINT_PROFILES),
        help="Endpoint profile to describe.",
    )
    parser.add_argument(
        "--endpoint",
        required=True,
        help="SPARQL endpoint URL used to collect statistics.",
    )
    parser.add_argument(
        "--public-endpoint",
        help="SPARQL endpoint URL to write in RDF when it differs from --endpoint.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output Turtle file.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="SPARQL query timeout in seconds (default: 3600).",
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover
    args = parse_args()
    public_endpoint = args.public_endpoint if args.public_endpoint else args.endpoint
    write_service_description(
        dataset_name=args.dataset,
        endpoint=args.endpoint,
        public_endpoint=public_endpoint,
        output=args.output,
        timeout=args.timeout,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
