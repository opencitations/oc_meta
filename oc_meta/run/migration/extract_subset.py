#!/usr/bin/env python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

# -*- coding: utf-8 -*-

import argparse
import gzip
import sys
from urllib.parse import urlparse

import rdflib
from rdflib.term import Node
from rich_argparse import RichHelpFormatter
from sparqlite import SPARQLClient

CHUNK_SIZE = 20


def get_subjects_of_class(client: SPARQLClient, class_uri: str, limit: int) -> list[str]:
    query = f"""
    SELECT ?s
    WHERE {{
        ?s a <{class_uri}> .
    }}
    LIMIT {limit}
    """
    results = client.query(query)
    return [result["s"]["value"] for result in results["results"]["bindings"]]


def load_entities_from_file(entities_file: str) -> list[str]:
    with open(entities_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def parse_object(result: dict[str, dict[str, str]]) -> rdflib.URIRef | rdflib.BNode | rdflib.Literal:
    o_value = result["o"]["value"]
    o_type = result["o"]["type"]
    if o_type == 'uri':
        return rdflib.URIRef(o_value)
    if o_type == 'bnode':
        return rdflib.BNode(o_value)
    if 'datatype' in result["o"]:
        return rdflib.Literal(o_value, datatype=result["o"]["datatype"])
    if 'xml:lang' in result["o"]:
        return rdflib.Literal(o_value, lang=result["o"]["xml:lang"])
    return rdflib.Literal(o_value)


def get_triples_for_entities(
    client: SPARQLClient,
    entity_uris: list[str],
    use_graphs: bool,
) -> list[tuple[rdflib.URIRef, rdflib.URIRef, Node, rdflib.URIRef | None]]:
    quads: list[tuple[rdflib.URIRef, rdflib.URIRef, Node, rdflib.URIRef | None]] = []

    for i in range(0, len(entity_uris), CHUNK_SIZE):
        chunk = entity_uris[i:i + CHUNK_SIZE]
        values = " ".join(f"<{uri}>" for uri in chunk)

        if use_graphs:
            query = f"""
            SELECT ?s ?p ?o ?g
            WHERE {{
                GRAPH ?g {{
                    VALUES ?s {{ {values} }}
                    ?s ?p ?o .
                }}
            }}
            """
        else:
            query = f"""
            SELECT ?s ?p ?o
            WHERE {{
                VALUES ?s {{ {values} }}
                ?s ?p ?o .
            }}
            """

        results = client.query(query)
        for result in results["results"]["bindings"]:
            s_term = rdflib.URIRef(result["s"]["value"])
            p_term = rdflib.URIRef(result["p"]["value"])
            o_term = parse_object(result)
            g_term = rdflib.URIRef(result["g"]["value"]) if "g" in result else None
            quads.append((s_term, p_term, o_term, g_term))

    return quads


def extract_subset(
    endpoint: str,
    limit: int,
    output_file: str,
    compress: bool,
    max_retries: int = 5,
    class_uri: str | None = None,
    entities_file: str | None = None,
    use_graphs: bool = True,
) -> tuple[int, str]:
    with SPARQLClient(endpoint, max_retries=max_retries, backoff_factor=2, timeout=3600) as client:
        if entities_file:
            subjects = load_entities_from_file(entities_file)
        else:
            assert class_uri is not None
            subjects = get_subjects_of_class(client, class_uri, limit)

        processed_entities: set[str] = set()
        pending_entities = set(subjects)

        dataset: rdflib.Dataset | None = None
        graph: rdflib.Graph | None = None
        if use_graphs:
            dataset = rdflib.Dataset()
        else:
            graph = rdflib.Graph()

        while pending_entities:
            batch = sorted(pending_entities - processed_entities)
            if not batch:
                break  # pragma: no cover

            processed_entities.update(batch)
            pending_entities.clear()

            quads = get_triples_for_entities(client, batch, use_graphs)

            for s_term, p_term, o_term, g_term in quads:
                if dataset is not None:
                    named_graph = dataset.graph(g_term)
                    named_graph.add((s_term, p_term, o_term))
                elif graph is not None:
                    graph.add((s_term, p_term, o_term))

                if isinstance(o_term, rdflib.URIRef):
                    o_str = str(o_term)
                    if o_str not in processed_entities:
                        pending_entities.add(o_str)

    store = dataset if dataset is not None else graph
    assert store is not None
    output_format = "nquads" if use_graphs else "nt"
    if compress:
        if not output_file.endswith('.gz'):
            output_file = output_file + '.gz'
        with gzip.open(output_file, 'wb') as f:
            store.serialize(destination=f, format=output_format)  # type: ignore[arg-type]
    else:
        store.serialize(destination=output_file, format=output_format)

    return len(processed_entities), output_file


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(
        description='Extract a subset of data from a SPARQL endpoint',
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument('--endpoint', default='http://localhost:8890/sparql',
                        help='SPARQL endpoint URL (default: http://localhost:8890/sparql)')

    discovery = parser.add_mutually_exclusive_group()
    discovery.add_argument('--class', dest='class_uri',
                           help='Class URI to extract instances of (default: fabio:Expression)')
    discovery.add_argument('--entities-file', dest='entities_file',
                           help='File with entity URIs to extract (one per line)')

    parser.add_argument('--limit', type=int, default=1000,
                        help='Maximum number of initial entities to process (default: 1000)')
    parser.add_argument('--output', default='output.nq',
                        help='Output file name (default: output.nq)')
    parser.add_argument('--compress', action='store_true',
                        help='Compress output file using gzip')
    parser.add_argument('--retries', type=int, default=5,
                        help='Maximum number of retries for failed queries (default: 5)')
    parser.add_argument('--no-graphs', action='store_true',
                        help='Disable named graph queries and output N-Triples instead of N-Quads')

    args = parser.parse_args()

    if not args.class_uri and not args.entities_file:
        args.class_uri = 'http://purl.org/spar/fabio/Expression'

    try:
        parsed_url = urlparse(args.endpoint)
        if not all([parsed_url.scheme, parsed_url.netloc]):
            raise ValueError("Invalid endpoint URL")
    except Exception:
        print(f"Error: Invalid endpoint URL: {args.endpoint}")
        return 1

    try:
        entity_count, final_output_file = extract_subset(
            args.endpoint,
            args.limit,
            args.output,
            args.compress,
            args.retries,
            class_uri=args.class_uri,
            entities_file=args.entities_file,
            use_graphs=not args.no_graphs,
        )

        print(f"Extraction complete. Processed {entity_count} entities.")
        print(f"Output saved to {final_output_file}")

        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
