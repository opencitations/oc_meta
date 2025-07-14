#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import gzip
import sys
import time
from urllib.parse import urlparse
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions
import rdflib


def execute_sparql_query(sparql, max_retries=5, retry_delay=2):
    """Execute a SPARQL query with retry mechanism"""
    retries = 0
    while retries < max_retries:
        try:
            return sparql.query().convert()
        except (SPARQLExceptions.EndPointInternalError, SPARQLExceptions.EndPointNotFound, 
                SPARQLExceptions.QueryBadFormed, Exception) as e:
            retries += 1
            if retries >= max_retries:
                raise Exception(f"Failed after {max_retries} retries: {str(e)}")
            print(f"Query failed, retrying ({retries}/{max_retries}): {str(e)}")
            time.sleep(retry_delay * retries)  # Exponential backoff


def get_subjects_of_class(endpoint, class_uri, limit, max_retries=5):
    """Get subjects that are instances of the specified class"""
    sparql = SPARQLWrapper(endpoint)
    query = f"""
    SELECT ?s
    WHERE {{
        ?s a <{class_uri}> .
    }}
    LIMIT {limit}
    """
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = execute_sparql_query(sparql, max_retries)
    
    return [result["s"]["value"] for result in results["results"]["bindings"]]


def get_triples_for_entity(endpoint, entity_uri, max_retries=5):
    """Get all triples where the entity is a subject and return RDF terms"""
    sparql = SPARQLWrapper(endpoint)
    query = f"""
    SELECT ?p ?o ?g
    WHERE {{
        GRAPH ?g {{
            <{entity_uri}> ?p ?o .
        }}
    }}
    """
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = execute_sparql_query(sparql, max_retries)
    
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
    subjects = get_subjects_of_class(endpoint, class_uri, limit, max_retries)
    processed_entities = set()
    pending_entities = set(subjects)
    
    dataset = rdflib.Dataset()
    
    while pending_entities:
        entity = pending_entities.pop()
        if entity in processed_entities:
            continue
        
        processed_entities.add(entity)
        
        quads = get_triples_for_entity(endpoint, entity, max_retries)
        
        for s_term, p_term, o_term, g_term in quads:
            graph = dataset.graph(g_term)
            graph.add((s_term, p_term, o_term))
            
            if isinstance(o_term, rdflib.URIRef):
                pending_entities.add(str(o_term))
    
    if compress:
        with gzip.open(output_file, 'wb') as f:
            dataset.serialize(destination=f, format='nquads')
    else:
        dataset.serialize(destination=output_file, format='nquads')
    
    return len(processed_entities)


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
        entity_count = extract_subset(
            args.endpoint, 
            args.class_uri, 
            args.limit, 
            args.output, 
            args.compress,
            args.retries
        )
        
        print(f"Extraction complete. Processed {entity_count} entities.")
        if args.compress:
            print(f"Output saved to {args.output}.gz")
        else:
            print(f"Output saved to {args.output}")
        
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
