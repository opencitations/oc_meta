import argparse
import csv
import os
import random
import time
import zipfile
from functools import partial
from multiprocessing import Pool, cpu_count

import yaml
from oc_meta.plugins.editor import MetaEditor
from rdflib import RDF, ConjunctiveGraph, Literal, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm


DATACITE = "http://purl.org/spar/datacite/"
LITERAL_REIFICATION = "http://www.essepuntato.it/2010/06/literalreification/"

def read_csv(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def sparql_query_with_retry(sparql, max_retries=3, initial_delay=1, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (backoff_factor ** attempt)
            time.sleep(delay + random.uniform(0, 1))

def check_entity_file(file_path, entity_uri, is_surviving):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as file:
                g = ConjunctiveGraph()
                g.parse(file, format='json-ld')
                entity = URIRef(entity_uri)
                
                if (entity, None, None) not in g:
                    if is_surviving:
                        tqdm.write(f"Error in file {file_path}: Surviving entity {entity_uri} does not exist")
                    return
                
                if not is_surviving:
                    tqdm.write(f"Error in file {file_path}: Merged entity {entity_uri} still exists")
                    return
                
                types = list(g.objects(entity, RDF.type))
                if not types:
                    tqdm.write(f"Error in file {file_path}: Entity {entity_uri} has no type")
                
                if URIRef(DATACITE + "Identifier") in types:
                    identifier_scheme = list(g.objects(entity, URIRef(DATACITE + "usesIdentifierScheme")))
                    literal_value = list(g.objects(entity, URIRef(LITERAL_REIFICATION + "hasLiteralValue")))
                    
                    if len(identifier_scheme) != 1:
                        tqdm.write(f"Error in file {file_path}: Entity {entity_uri} should have exactly one usesIdentifierScheme, found {len(identifier_scheme)}")
                    elif not isinstance(identifier_scheme[0], URIRef):
                        tqdm.write(f"Error in file {file_path}: Entity {entity_uri}'s usesIdentifierScheme should be a URIRef, found {type(identifier_scheme[0])}")
                    
                    if len(literal_value) != 1:
                        tqdm.write(f"Error in file {file_path}: Entity {entity_uri} should have exactly one hasLiteralValue, found {len(literal_value)}")
                    elif not isinstance(literal_value[0], Literal):
                        tqdm.write(f"Error in file {file_path}: Entity {entity_uri}'s hasLiteralValue should be a Literal, found {type(literal_value[0])}")

def check_entity_sparql(sparql_endpoint, entity_uri, is_surviving):
    sparql = SPARQLWrapper(sparql_endpoint)
    
    # Query to check if the entity exists
    exists_query = f"""
    ASK {{
        <{entity_uri}> ?p ?o .
    }}
    """
    sparql.setQuery(exists_query)
    sparql.setReturnFormat(JSON)
    exists_results = sparql_query_with_retry(sparql)

    if exists_results['boolean']:
        if not is_surviving:
            tqdm.write(f"Error in SPARQL: Merged entity {entity_uri} still exists")
    else:
        if is_surviving:
            tqdm.write(f"Error in SPARQL: Surviving entity {entity_uri} does not exist")
        return

    # Query to get entity types
    types_query = f"""
    SELECT ?type WHERE {{
        <{entity_uri}> a ?type .
    }}
    """
    sparql.setQuery(types_query)
    sparql.setReturnFormat(JSON)
    types_results = sparql_query_with_retry(sparql)

    types = [result['type']['value'] for result in types_results['results']['bindings']]
    if not types:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has no type")

    if DATACITE + "Identifier" in types:
        # Query for identifier scheme and literal value
        identifier_query = f"""
        SELECT ?scheme ?value WHERE {{
            <{entity_uri}> <{DATACITE}usesIdentifierScheme> ?scheme .
            <{entity_uri}> <{LITERAL_REIFICATION}hasLiteralValue> ?value .
        }}
        """
        sparql.setQuery(identifier_query)
        sparql.setReturnFormat(JSON)
        identifier_results = sparql_query_with_retry(sparql)

        schemes = [result['scheme']['value'] for result in identifier_results['results']['bindings']]
        values = [result['value']['value'] for result in identifier_results['results']['bindings']]

        if len(schemes) != 1:
            tqdm.write(f"Error in SPARQL: Entity {entity_uri} should have exactly one usesIdentifierScheme, found {len(schemes)}")
        elif not schemes[0].startswith('http'):
            tqdm.write(f"Error in SPARQL: Entity {entity_uri}'s usesIdentifierScheme should be a URIRef, found {schemes[0]}")

        if len(values) != 1:
            tqdm.write(f"Error in SPARQL: Entity {entity_uri} should have exactly one hasLiteralValue, found {len(values)}")

def process_csv(args, csv_file):
    csv_path, rdf_dir, meta_editor, sparql_endpoint = args
    csv_path = os.path.join(csv_path, csv_file)
    data = read_csv(csv_path)
    tasks = []

    for row in data:
        if 'Done' not in row or row['Done'] != 'True':
            continue

        surviving_entity = row['surviving_entity']
        merged_entities = row['merged_entities'].split('; ')
        all_entities = [surviving_entity] + merged_entities

        for entity in all_entities:
            file_path = meta_editor.find_file(rdf_dir, meta_editor.dir_split, meta_editor.n_file_item, entity, True)
            tasks.append((entity, entity == surviving_entity, file_path, rdf_dir, meta_editor, sparql_endpoint))

    return tasks

def process_entity(args):
    entity, is_surviving, file_path, rdf_dir, meta_editor, sparql_endpoint = args

    if file_path is None:
        tqdm.write(f"Error: Could not find file for entity {entity}")
    else:
        check_entity_file(file_path, entity, is_surviving)

    check_entity_sparql(sparql_endpoint, entity, is_surviving)

def main():
    parser = argparse.ArgumentParser(description="Check merge process success on files and SPARQL endpoint")
    parser.add_argument('csv_folder', type=str, help="Path to the folder containing CSV files")
    parser.add_argument('rdf_dir', type=str, help="Path to the RDF directory")
    parser.add_argument('--meta_config', type=str, required=True, help="Path to meta configuration file")
    args = parser.parse_args()

    meta_editor = MetaEditor(args.meta_config, "")

    with open(args.meta_config, 'r') as config_file:
        config = yaml.safe_load(config_file)

    sparql_endpoint = config['triplestore_url']

    csv_files = [f for f in os.listdir(args.csv_folder) if f.endswith('.csv')]
    
    # Process CSV files in parallel
    with Pool(processes=cpu_count()) as pool:
        process_csv_partial = partial(process_csv, (args.csv_folder, args.rdf_dir, meta_editor, sparql_endpoint))
        all_tasks = list(tqdm(pool.imap(process_csv_partial, csv_files), total=len(csv_files), desc="Processing CSV files"))
    
    # Flatten the list of lists into a single list
    all_tasks = [task for sublist in all_tasks for task in sublist]

    # Process entities in parallel
    with Pool(processes=cpu_count()) as pool:
        list(tqdm(pool.imap(process_entity, all_tasks), total=len(all_tasks), desc="Processing entities"))

if __name__ == "__main__":
    main()
