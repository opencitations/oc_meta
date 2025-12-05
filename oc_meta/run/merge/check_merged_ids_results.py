import argparse
import csv
import os
import re
import zipfile
from functools import partial
from multiprocessing import Pool, cpu_count

import yaml
from oc_meta.plugins.editor import MetaEditor
from rdflib import RDF, Dataset, Literal, Namespace, URIRef
from sparqlite import SPARQLClient
from tqdm import tqdm

DATACITE = "http://purl.org/spar/datacite/"
LITERAL_REIFICATION = "http://www.essepuntato.it/2010/06/literalreification/"
PROV = Namespace("http://www.w3.org/ns/prov#")

def read_csv(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def check_provenance(prov_file_path, entity_uri, is_surviving):
    def extract_snapshot_number(snapshot_uri):
        match = re.search(r'/prov/se/(\d+)$', str(snapshot_uri))
        if match:
            return int(match.group(1))
        return 0  # Return 0 if no match found, this will put invalid URIs at the start

    def is_merge_snapshot(g: Dataset, snapshot):
        description = g.value(snapshot, URIRef("http://purl.org/dc/terms/description"))
        if description:
            # Check if the description indicates a merge operation
            return "has been merged with" in str(description)
        return False

    try:
        with zipfile.ZipFile(prov_file_path, 'r') as zip_ref:
            g = Dataset(default_union=True)
            for filename in zip_ref.namelist():
                with zip_ref.open(filename) as file:
                    g.parse(file, format='json-ld')
            
            entity = URIRef(entity_uri)
            snapshots = list(g.subjects(PROV.specializationOf, entity))
            
            if len(snapshots) <= 1:
                tqdm.write(f"Error in provenance file {prov_file_path}: Less than two snapshots found for entity {entity_uri}")
                return
            
            # Sort snapshots by their URI number
            snapshots.sort(key=extract_snapshot_number)
            
            for i, snapshot in enumerate(snapshots):
                snapshot_number = extract_snapshot_number(snapshot)
                if snapshot_number != i + 1:
                    tqdm.write(f"Error in provenance file {prov_file_path}: Snapshot {snapshot} has unexpected number {snapshot_number}, expected {i + 1}")
                
                gen_time = g.value(snapshot, PROV.generatedAtTime)
                if gen_time is None:
                    tqdm.write(f"Error in provenance file {prov_file_path}: Snapshot {snapshot} has no generation time")
                
                if i < len(snapshots) - 1 or not is_surviving:
                    invalidation_time = g.value(snapshot, PROV.invalidatedAtTime)
                    if invalidation_time is None:
                        tqdm.write(f"Error in provenance file {prov_file_path}: Non-last snapshot {snapshot} has no invalidation time")
                elif is_surviving and g.value(snapshot, PROV.invalidatedAtTime) is not None:
                    tqdm.write(f"Error in provenance file {prov_file_path}: Last snapshot of surviving entity {snapshot} should not have invalidation time")
                
                # Check prov:wasDerivedFrom
                derived_from = list(g.objects(snapshot, PROV.wasDerivedFrom))
                if i == 0:  # First snapshot
                    if derived_from:
                        tqdm.write(f"Error in provenance file {prov_file_path}: First snapshot {snapshot} should not have prov:wasDerivedFrom relation")
                else:  # All other snapshots
                    # Check if this is a merge snapshot
                    is_merge = is_merge_snapshot(g, snapshot)
                    
                    if is_merge:
                        if len(derived_from) < 2:
                            tqdm.write(f"Error in provenance file {prov_file_path}: Merge snapshot {snapshot} should be derived from more than one snapshot")
                    else:
                        if len(derived_from) != 1:
                            tqdm.write(f"Error in provenance file {prov_file_path}: Non-merge snapshot {snapshot} should have exactly one prov:wasDerivedFrom relation, but has {len(derived_from)}")
                        elif derived_from[0] != snapshots[i-1]:
                            tqdm.write(f"Error in provenance file {prov_file_path}: Snapshot {snapshot} is not derived from the previous snapshot")
            
            if not is_surviving:
                # Check if the last snapshot is invalidated for merged entities
                last_snapshot = snapshots[-1]
                if (None, PROV.invalidated, last_snapshot) not in g:
                    tqdm.write(f"Error in provenance file {prov_file_path}: Last snapshot {last_snapshot} of merged entity {entity_uri} is not invalidated")
    
    except FileNotFoundError:
        tqdm.write(f"Error: Provenance file not found for entity {entity_uri}")
    except zipfile.BadZipFile:
        tqdm.write(f"Error: Invalid zip file for provenance of entity {entity_uri}")

def check_entity_file(file_path, entity_uri, is_surviving):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as file:
                g = Dataset(default_union=True)
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
    
    # Check provenance
    prov_file_path = file_path.replace('.zip', '') + '/prov/se.zip'
    check_provenance(prov_file_path, entity_uri, is_surviving)

def check_entity_sparql(client, entity_uri, is_surviving):
    has_issues = False

    # Query to check if the entity exists
    exists_query = f"""
    ASK {{
        <{entity_uri}> ?p ?o .
    }}
    """
    exists_results = client.query(exists_query)

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
        referenced_results = client.query(referenced_query)

        if referenced_results['boolean']:
            tqdm.write(f"Error in SPARQL: Merged entity {entity_uri} is still referenced by other entities")
            has_issues = True

    # Query to get entity types
    types_query = f"""
    SELECT ?type WHERE {{
        <{entity_uri}> a ?type .
    }}
    """
    types_results = client.query(types_query)

    types = [result['type']['value'] for result in types_results['results']['bindings']]
    if not types:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has no type")
        has_issues = True

    if DATACITE + "Identifier" in types:
        # Query for identifier scheme and literal value
        identifier_query = f"""
        SELECT ?scheme ?value WHERE {{
            <{entity_uri}> <{DATACITE}usesIdentifierScheme> ?scheme .
            <{entity_uri}> <{LITERAL_REIFICATION}hasLiteralValue> ?value .
        }}
        """
        identifier_results = client.query(identifier_query)

        schemes = [result['scheme']['value'] for result in identifier_results['results']['bindings']]
        values = [result['value']['value'] for result in identifier_results['results']['bindings']]

        if len(schemes) != 1:
            tqdm.write(f"Error in SPARQL: Entity {entity_uri} should have exactly one usesIdentifierScheme, found {len(schemes)}")
            has_issues = True
        elif not schemes[0].startswith('http'):
            tqdm.write(f"Error in SPARQL: Entity {entity_uri}'s usesIdentifierScheme should be a URIRef, found {schemes[0]}")
            has_issues = True

        if len(values) != 1:
            tqdm.write(f"Error in SPARQL: Entity {entity_uri} should have exactly one hasLiteralValue, found {len(values)}")
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

def get_entity_triples(client, entity_uri):
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
    results = client.query(query)

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

def process_entity(args):
    entity, is_surviving, file_path, rdf_dir, meta_config_path, sparql_endpoint, query_output_dir, surviving_entity = args

    if file_path is None:
        tqdm.write(f"Error: Could not find file for entity {entity}")
    else:
        check_entity_file(file_path, entity, is_surviving)

    with SPARQLClient(sparql_endpoint, max_retries=3, backoff_factor=1) as client:
        has_issues = check_entity_sparql(client, entity, is_surviving)

        if has_issues and not is_surviving:
            triples = get_entity_triples(client, entity)
            combined_query = generate_update_query(entity, surviving_entity, triples)

            query_file_path = os.path.join(query_output_dir, f"update_{entity.split('/')[-1]}.sparql")
            with open(query_file_path, 'w') as f:
                f.write(combined_query)

def main():
    parser = argparse.ArgumentParser(description="Check merge process success on files and SPARQL endpoint")
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