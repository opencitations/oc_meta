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
FABIO = "http://purl.org/spar/fabio/"

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
                elif len(types) > 2:
                    tqdm.write(f"Error in file {file_path}: Entity {entity_uri} has more than two types")
                elif URIRef(FABIO + "Expression") not in types:
                    tqdm.write(f"Error in file {file_path}: Entity {entity_uri} is not a fabio:Expression")
                
                identifiers = list(g.objects(entity, URIRef(DATACITE + "hasIdentifier")))
                if not identifiers:
                    tqdm.write(f"Error in file {file_path}: Entity {entity_uri} has no datacite:hasIdentifier")

def check_entity_sparql(sparql_endpoint, entity_uri, is_surviving):
    sparql = SPARQLWrapper(sparql_endpoint)
    has_issues = False
    
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
            has_issues = True
    else:
        if is_surviving:
            tqdm.write(f"Error in SPARQL: Surviving entity {entity_uri} does not exist")
            has_issues = True
        return has_issues

    if not is_surviving:
        referenced_query = f"""
        ASK {{
            ?s ?p <{entity_uri}> .
        }}
        """
        sparql.setQuery(referenced_query)
        sparql.setReturnFormat(JSON)
        referenced_results = sparql_query_with_retry(sparql)

        if referenced_results['boolean']:
            tqdm.write(f"Error in SPARQL: Merged entity {entity_uri} is still referenced by other entities")
            has_issues = True

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
        has_issues = True
    elif len(types) > 2:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has more than two types")
        has_issues = True
    elif FABIO + "Expression" not in types:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} is not a fabio:Expression")
        has_issues = True

    # Query for identifiers
    identifiers_query = f"""
    SELECT ?identifier WHERE {{
        <{entity_uri}> <{DATACITE}hasIdentifier> ?identifier .
    }}
    """
    sparql.setQuery(identifiers_query)
    sparql.setReturnFormat(JSON)
    identifiers_results = sparql_query_with_retry(sparql)

    identifiers = [result['identifier']['value'] for result in identifiers_results['results']['bindings']]
    if not identifiers:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has no datacite:hasIdentifier")
        has_issues = True

    return has_issues

def process_csv(args, csv_file):
    csv_path, rdf_dir, meta_config_path, sparql_endpoint, query_output_dir = args
    csv_path = os.path.join(csv_path, csv_file)
    data = read_csv(csv_path)
    tasks = []

    meta_editor = MetaEditor(meta_config_path, "")

    for row in data:
        if 'Done' not in row or row['Done'] != 'True':
            continue

        surviving_entity = row['surviving_entity']
        merged_entities = row['merged_entities'].split('; ')
        all_entities = [surviving_entity] + merged_entities

        for entity in all_entities:
            file_path = meta_editor.find_file(rdf_dir, meta_editor.dir_split, meta_editor.n_file_item, entity, True)
            tasks.append((entity, entity == surviving_entity, file_path, rdf_dir, meta_config_path, sparql_endpoint, query_output_dir, surviving_entity))

    return tasks

def process_entity(args):
    entity, is_surviving, file_path, rdf_dir, meta_config_path, sparql_endpoint, query_output_dir, surviving_entity = args

    if file_path is None:
        tqdm.write(f"Error: Could not find file for entity {entity}")
    else:
        check_entity_file(file_path, entity, is_surviving)

    # has_issues = check_entity_sparql(sparql_endpoint, entity, is_surviving)

    # if has_issues and not is_surviving:
    #     triples = get_entity_triples(sparql_endpoint, entity)
    #     combined_query = generate_update_query(entity, surviving_entity, triples)
        
    #     # Save combined DELETE DATA and INSERT DATA query
    #     query_file_path = os.path.join(query_output_dir, f"update_{entity.split('/')[-1]}.sparql")
    #     with open(query_file_path, 'w') as f:
    #         f.write(combined_query)

def get_entity_triples(sparql_endpoint, entity_uri):
    sparql = SPARQLWrapper(sparql_endpoint)
    query = f"""
    SELECT ?g ?s ?p ?o
    WHERE {{
        GRAPH ?g {{
            {{
                <{entity_uri}> ?p ?o .
                BIND(<{entity_uri}> AS ?s)
            }}
            UNION
            {{
                ?s ?p <{entity_uri}> .
                BIND(<{entity_uri}> AS ?o)
            }}
        }}
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql_query_with_retry(sparql)
    
    triples = []
    for result in results["results"]["bindings"]:
        graph = result["g"]["value"]
        subject = URIRef(result["s"]["value"])
        predicate = URIRef(result["p"]["value"])
        
        obj_data = result["o"]
        if obj_data["type"] == "uri":
            obj = URIRef(obj_data["value"])
        else:
            datatype = obj_data.get("datatype")
            obj = Literal(obj_data["value"], datatype=URIRef(datatype) if datatype else None)
        
        triples.append((graph, subject, predicate, obj))
    
    return triples

def generate_update_query(merged_entity, surviving_entity, triples):
    delete_query = "DELETE DATA {\n"
    insert_query = "INSERT DATA {\n"
    
    for graph, subject, predicate, obj in triples:
        if subject == URIRef(merged_entity):
            delete_query += f"  GRAPH <{graph}> {{ <{subject}> <{predicate}> {obj.n3()} }}\n"
        elif obj == URIRef(merged_entity):
            delete_query += f"  GRAPH <{graph}> {{ <{subject}> <{predicate}> <{obj}> }}\n"
            insert_query += f"  GRAPH <{graph}> {{ <{subject}> <{predicate}> <{surviving_entity}> }}\n"
    
    delete_query += "}\n"
    insert_query += "}\n"
    
    combined_query = delete_query + "\n" + insert_query
    return combined_query

def main():
    parser = argparse.ArgumentParser(description="Check merge process success on files and SPARQL endpoint for bibliographic resources")
    parser.add_argument('csv_folder', type=str, help="Path to the folder containing CSV files")
    parser.add_argument('rdf_dir', type=str, help="Path to the RDF directory")
    parser.add_argument('--meta_config', type=str, required=True, help="Path to meta configuration file")
    parser.add_argument('--query_output', type=str, required=True, help="Path to the folder where SPARQL queries will be saved")
    args = parser.parse_args()

    with open(args.meta_config, 'r') as config_file:
        config = yaml.safe_load(config_file)

    sparql_endpoint = config['triplestore_url']

    os.makedirs(args.query_output, exist_ok=True)

    csv_files = [f for f in os.listdir(args.csv_folder) if f.endswith('.csv')]
    
    # Process CSV files in parallel
    with Pool(processes=cpu_count()) as pool:
        process_csv_partial = partial(process_csv, (args.csv_folder, args.rdf_dir, args.meta_config, sparql_endpoint, args.query_output))
        all_tasks = list(tqdm(pool.imap(process_csv_partial, csv_files), total=len(csv_files), desc="Processing CSV files"))
    
    # Flatten the list of lists into a single list
    all_tasks = [task for sublist in all_tasks for task in sublist]

    # Process entities in parallel
    with Pool(processes=cpu_count()) as pool:
        list(tqdm(pool.imap(process_entity, all_tasks), total=len(all_tasks), desc="Processing entities"))

if __name__ == "__main__":
    main()