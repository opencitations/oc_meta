#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import gzip
import sys
from urllib.parse import urlparse

import rdflib
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


def get_triples_for_entity(client, entity_uri):
    """Get all triples where the entity is a subject and return RDF terms"""
    query = f"""
    SELECT ?p ?o ?g
    WHERE {{
        GRAPH ?g {{
            <{entity_uri}> ?p ?o .
        }}
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
        elif o_type in {'literal', 'typed-literal'}:
            if 'datatype' in result["o"]:
                datatype = result["o"]["datatype"]
                o_term = rdflib.Literal(o_value, datatype=datatype)
            elif 'xml:lang' in result["o"]:
                lang = result["o"]["xml:lang"]
                o_term = rdflib.Literal(o_value, lang=lang)
            else:
                o_term = rdflib.Literal(o_value)

        quads.append((s_term, p_term, o_term, g_term))

    return quads


def extract_subset(endpoint, class_uri, limit, output_file, compress, max_retries=5):
    """Extract a subset of the SPARQL endpoint data"""
    with SPARQLClient(endpoint, max_retries=max_retries, backoff_factor=2) as client:
        subjects = get_subjects_of_class(client, class_uri, limit)
        processed_entities = set()
        pending_entities = set(subjects)

        dataset = rdflib.Dataset()

        while pending_entities:
            entity = pending_entities.pop()
            if entity in processed_entities:
                continue

            processed_entities.add(entity)

            quads = get_triples_for_entity(client, entity)

            for s_term, p_term, o_term, g_term in quads:
                graph = dataset.graph(g_term)
                graph.add((s_term, p_term, o_term))

                if isinstance(o_term, rdflib.URIRef):
                    pending_entities.add(str(o_term))

    if compress:
        if not output_file.endswith('.gz'):
            output_file = output_file + '.gz'
        with gzip.open(output_file, 'wb') as f:
            dataset.serialize(destination=f, format='nquads')
    else:
        dataset.serialize(destination=output_file, format='nquads')

    return len(processed_entities), output_file


def main():
    parser = argparse.ArgumentParser(description='Extract a subset of data from a SPARQL endpoint in N-Quads format')
    parser.add_argument('--endpoint', default='http://localhost:8890/sparql', 
                        help='SPARQL endpoint URL (default: http://localhost:8890/sparql)')
    parser.add_argument('--class', dest='class_uri', default='http://purl.org/spar/fabio/Expression',
                        help='Class URI to extract instances of (default: http://purl.org/spar/fabio/Expression)')
    parser.add_argument('--limit', type=int, default=1000,
                        help='Maximum number of initial entities to process (default: 1000)')
    parser.add_argument('--output', default='output.nq',
                        help='Output file name (default: output.nq)')
    parser.add_argument('--compress', action='store_true',
                        help='Compress output file using gzip')
    parser.add_argument('--retries', type=int, default=5,
                        help='Maximum number of retries for failed queries (default: 5)')
    
    args = parser.parse_args()
    
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
            args.class_uri, 
            args.limit, 
            args.output, 
            args.compress,
            args.retries
        )
        
        print(f"Extraction complete. Processed {entity_count} entities.")
        print(f"Output saved to {final_output_file}")
        
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
