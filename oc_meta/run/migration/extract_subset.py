#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import gzip
import sys
from urllib.parse import urlparse

import rdflib
from rich_argparse import RichHelpFormatter
from sparqlite import SPARQLClient


def get_subjects_of_class(client, class_uri, limit):
    """Get subjects that are instances of the specified class"""
    query = f"""
    SELECT ?s
    WHERE {{
        ?s a <{class_uri}> .
    }}
    LIMIT {limit}
    """

    results = client.query(query)

    return [result["s"]["value"] for result in results["results"]["bindings"]]


def get_subjects_by_predicate(client, predicate_uri, limit):
    query = f"""
    SELECT ?s
    WHERE {{
        ?s <{predicate_uri}> ?o .
    }}
    LIMIT {limit}
    """
    results = client.query(query)
    return [result["s"]["value"] for result in results["results"]["bindings"]]


def get_triples_for_entity(client, entity_uri, use_graphs=True):
    if use_graphs:
        query = f"""
        SELECT ?p ?o ?g
        WHERE {{
            GRAPH ?g {{
                <{entity_uri}> ?p ?o .
            }}
        }}
        """
    else:
        query = f"""
        SELECT ?p ?o
        WHERE {{
            <{entity_uri}> ?p ?o .
        }}
        """

    results = client.query(query)

    s_term = rdflib.URIRef(entity_uri)
    quads = []

    for result in results["results"]["bindings"]:
        p_value = result["p"]["value"]
        p_term = rdflib.URIRef(p_value)

        o_value = result["o"]["value"]
        o_type = result["o"]["type"]

        g_term = None
        if "g" in result:
            g_value = result["g"]["value"]
            g_term = rdflib.URIRef(g_value)

        if o_type == 'uri':
            o_term = rdflib.URIRef(o_value)
        elif o_type == 'bnode':
            o_term = rdflib.BNode(o_value)
        elif o_type in {'literal', 'typed-literal'}:
            if 'datatype' in result["o"]:
                datatype = result["o"]["datatype"]
                o_term = rdflib.Literal(o_value, datatype=datatype)
            elif 'xml:lang' in result["o"]:
                lang = result["o"]["xml:lang"]
                o_term = rdflib.Literal(o_value, lang=lang)
            else:
                o_term = rdflib.Literal(o_value)
        else:
            o_term = rdflib.Literal(o_value)

        quads.append((s_term, p_term, o_term, g_term))

    return quads


def extract_subset(
    endpoint, limit, output_file, compress, max_retries=5,
    class_uri=None, predicate_uri=None, use_graphs=True, recurse=True,
):
    with SPARQLClient(endpoint, max_retries=max_retries, backoff_factor=2, timeout=3600) as client:
        if predicate_uri:
            subjects = get_subjects_by_predicate(client, predicate_uri, limit)
        else:
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
            entity = pending_entities.pop()
            if entity in processed_entities:
                continue

            processed_entities.add(entity)

            quads = get_triples_for_entity(client, entity, use_graphs)

            for s_term, p_term, o_term, g_term in quads:
                if dataset is not None:
                    named_graph = dataset.graph(g_term)
                    named_graph.add((s_term, p_term, o_term))
                elif graph is not None:
                    graph.add((s_term, p_term, o_term))

                if recurse and isinstance(o_term, rdflib.URIRef):
                    pending_entities.add(str(o_term))

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
    discovery.add_argument('--predicate', dest='predicate_uri',
                           help='Predicate URI for entity discovery (alternative to --class)')

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
    parser.add_argument('--no-recurse', action='store_true',
                        help='Do not recursively follow URI objects')

    args = parser.parse_args()

    if not args.class_uri and not args.predicate_uri:
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
            predicate_uri=args.predicate_uri,
            use_graphs=not args.no_graphs,
            recurse=not args.no_recurse,
        )
        
        print(f"Extraction complete. Processed {entity_count} entities.")
        print(f"Output saved to {final_output_file}")
        
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
