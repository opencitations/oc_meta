import argparse
import csv
import os
import re
import zipfile
import time
from multiprocessing import Pool, cpu_count
from typing import Dict, List, Set
from functools import wraps

import yaml
from oc_meta.lib.master_of_regex import name_and_ids, semicolon_in_people_field
from rdflib import ConjunctiveGraph, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm


BATCH_SIZE = 10
def parse_identifiers(id_string: str) -> List[Dict[str, str]]:
    """
    Parse space-separated identifiers in the format schema:value
    Returns a list of dicts with 'schema' and 'value' keys.
    Handles values that may contain colons by splitting only at the first colon.
    """
    if not id_string or id_string.isspace():
        return []
        
    identifiers = []
    for identifier in id_string.strip().split():
        # Split only at first colon
        parts = identifier.split(':', 1)
        if len(parts) == 2:
            identifiers.append({
                'schema': parts[0].lower(),
                'value': parts[1]
            })
    return identifiers

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """
    Decorator per implementare retry con backoff esponenziale
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Initial delay in seconds
            delay = backoff_in_seconds
            last_exception = None
            
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if i == retries - 1:  # Last attempt
                        break
                    
                    time.sleep(delay)
                    # Exponential backoff
                    delay *= 2
            
            # Se arriviamo qui, tutti i tentativi sono falliti
            raise RuntimeError(f"All {retries} attempts failed") from last_exception
            
        return wrapper
    return decorator

@retry_with_backoff(retries=3, backoff_in_seconds=1)
def execute_sparql_query(sparql: SPARQLWrapper) -> dict:
    """
    Esegue una query SPARQL con retry
    """
    return sparql.query().convert()

def check_omids_existence(identifiers: List[Dict[str, str]], endpoint_url: str) -> Dict[str, Set[str]]:
    """
    Query SPARQL endpoint to find OMIDs for a single identifier
    Returns dict mapping identifier keys to sets of found OMIDs
    """
    if not identifiers:
        return {}
    
    found_omids = {}
    
    for identifier in identifiers:
        sparql = SPARQLWrapper(endpoint_url)
        id_key = f"{identifier['schema']}:{identifier['value']}"
                
        query = f"""
        PREFIX datacite: <http://purl.org/spar/datacite/>
        PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        
        SELECT DISTINCT ?omid
        WHERE {{
            {{
                ?omid literal:hasLiteralValue "{identifier['value']}"^^xsd:string ;
                     datacite:usesIdentifierScheme datacite:{identifier['schema']} .
            }}
            UNION
            {{
                ?omid literal:hasLiteralValue "{identifier['value']}" ;
                     datacite:usesIdentifierScheme datacite:{identifier['schema']} .
            }}
        }}
        """
                
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        try:
            results = execute_sparql_query(sparql)
            omids = set()
            
            for result in results["results"]["bindings"]:
                omid = result["omid"]["value"]
                omids.add(omid)
                        
            found_omids[id_key] = omids
            
        except Exception as e:
            print(f"SPARQL query failed for identifier {id_key}: {str(e)}")
            found_omids[id_key] = set()
    
    return found_omids

def find_file(rdf_dir: str, dir_split_number: int, items_per_file: int, uri: str, zip_output_rdf: bool) -> str|None:
    """Find the ZIP file containing the entity data"""
    entity_regex: str = r'^(https:\/\/w3id\.org\/oc\/meta)\/([a-z][a-z])\/(0[1-9]+0)?([1-9][0-9]*)$'
    entity_match = re.match(entity_regex, uri)
    if entity_match:
        cur_number = int(entity_match.group(4))
        cur_file_split: int = 0
        while True:
            if cur_number > cur_file_split:
                cur_file_split += items_per_file
            else:
                break
        cur_split: int = 0
        while True:
            if cur_number > cur_split:
                cur_split += dir_split_number
            else:
                break
        short_name = entity_match.group(2)
        sub_folder = entity_match.group(3)
        cur_dir_path = os.path.join(rdf_dir, short_name, sub_folder, str(cur_split))
        extension = '.zip' if zip_output_rdf else '.json'
        cur_file_path = os.path.join(cur_dir_path, str(cur_file_split)) + extension
        return cur_file_path
    return None

def find_prov_file(data_zip_path: str) -> str|None:
    """Find the provenance ZIP file associated with the data file"""
    try:
        base_dir = os.path.dirname(data_zip_path)
        file_name = os.path.splitext(os.path.basename(data_zip_path))[0]
        prov_dir = os.path.join(base_dir, file_name, 'prov')
        prov_file = os.path.join(prov_dir, 'se.zip')
        return prov_file if os.path.exists(prov_file) else None
        
    except Exception as e:
        print(f"Error finding provenance file for {data_zip_path}: {str(e)}")
        return None

def load_graph_from_zip(zip_path: str, omid: str, zip_cache: dict) -> tuple[bool, bool]:
    """
    Load RDF graph from data ZIP file and check if:
    1. The OMID exists in the data graph
    2. A provenance graph exists for this OMID
    Returns tuple of (data_found, prov_found) booleans
    """
    try:
        data_found = False
        prov_found = False
        omid_uri = URIRef(omid)
        
        # Check data graph
        if zip_path not in zip_cache:
            data_graph = ConjunctiveGraph()
            with zipfile.ZipFile(zip_path, 'r') as z:
                json_files = [f for f in z.namelist() if f.endswith('.json')]
                if json_files:
                    with z.open(json_files[0]) as f:
                        data_graph.parse(data=f.read(), format='json-ld')
            zip_cache[zip_path] = data_graph
        else:
            data_graph = zip_cache[zip_path]
        
        data_found = any(data_graph.triples((omid_uri, None, None)))
        
        # Check provenance graph
        prov_path = find_prov_file(zip_path)
        if prov_path:
            if prov_path not in zip_cache:
                prov_graph = ConjunctiveGraph()
                with zipfile.ZipFile(prov_path, 'r') as z:
                    json_files = [f for f in z.namelist() if f.endswith('.json')]
                    if json_files:
                        with z.open(json_files[0]) as f:
                            prov_graph.parse(data=f.read(), format='json-ld')
                zip_cache[prov_path] = prov_graph
            else:
                prov_graph = zip_cache[prov_path]
            
            prov_graph_uri = URIRef(f"{omid}/prov/")
            prov_found = any(prov_graph.triples((None, None, None), prov_graph_uri))
        
        return data_found, prov_found
        
    except Exception as e:
        print(f"Error loading graphs from {zip_path}: {str(e)}")
        return False, False

def process_csv_file(args: tuple) -> Dict:
    """
    Process a single CSV file and check its identifiers
    Returns statistics about processed rows and found/missing OMIDs
    """
    file_path, endpoint_url, root_dir = args
    stats = {
        'total_rows': 0,
        'rows_with_ids': 0,
        'total_identifiers': 0,
        'identifiers_with_omids': 0,
        'identifiers_without_omids': 0,
        'identifiers_details': [],
        'data_graphs_found': 0,
        'data_graphs_missing': 0,
        'prov_graphs_found': 0,
        'prov_graphs_missing': 0,
        'processed_omids': {}
    }
    
    # Cache per gli identificatori e risultati OMID
    identifier_cache = {}  # chiave: "schema:value", valore: set di OMID
    omid_results_cache = {}  # chiave: omid, valore: (data_found, prov_found)
    
    # Prima fase: raccolta di tutti gli identificatori dal file
    unique_identifiers = set()
    row_identifiers = []  # Lista di tuple (row_num, col, identifier)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1):
                stats['total_rows'] += 1
                row_has_ids = False
                
                # Process all columns that may contain identifiers
                id_columns = ['id', 'author', 'editor', 'publisher', 'venue']
                
                for col in id_columns:
                    identifiers = []
                    
                    if col == 'id':
                        if row[col]:
                            identifiers = parse_identifiers(row[col])
                    else:
                        if row[col]:
                            elements = re.split(semicolon_in_people_field, row[col])
                            for element in elements:
                                match = re.search(name_and_ids, element)
                                if match and match.group(2):
                                    ids_str = match.group(2)
                                    identifiers.extend(parse_identifiers(ids_str))
                    
                    if identifiers:
                        row_has_ids = True
                        stats['total_identifiers'] += len(identifiers)
                        for identifier in identifiers:
                            id_key = f"{identifier['schema']}:{identifier['value']}"
                            unique_identifiers.add(id_key)  # Aggiungiamo solo identificatori unici
                            row_identifiers.append((row_num, col, identifier))
                
                if row_has_ids:
                    stats['rows_with_ids'] += 1

        # Seconda fase: query SPARQL in batch
        # Split only at first colon for each identifier
        all_identifiers = []
        for id_key in unique_identifiers:
            parts = id_key.split(':', 1)
            if len(parts) == 2:
                all_identifiers.append({
                    'schema': parts[0].lower(),
                    'value': parts[1]
                })
        
        for i in range(0, len(all_identifiers), BATCH_SIZE):
            batch = all_identifiers[i:i + BATCH_SIZE]
            batch_results = check_omids_existence(batch, endpoint_url)
            identifier_cache.update(batch_results)

        # Terza fase: mappatura OMID -> file ZIP
        omids_by_file = {}  # chiave: zip_path, valore: set di OMID
        all_omids = set()
        
        for row_num, col, identifier in row_identifiers:
            id_key = f"{identifier['schema']}:{identifier['value']}"
            omids = identifier_cache.get(id_key, set())
            
            if omids:
                stats['identifiers_with_omids'] += 1
                all_omids.update(omids)
                
                # Raggruppa OMID per file
                for omid in omids:
                    if omid not in omid_results_cache:  # Skip se già controllato
                        zip_path = find_file(root_dir, 10000, 1000, omid, True)
                        if zip_path and os.path.exists(zip_path):
                            if zip_path not in omids_by_file:
                                omids_by_file[zip_path] = set()
                            omids_by_file[zip_path].add(omid)
            else:
                stats['identifiers_without_omids'] += 1
            
            stats['identifiers_details'].append({
                'schema': identifier['schema'],
                'value': identifier['value'],
                'column': col,
                'has_omid': bool(omids),
                'row_number': row_num,
                'file': file_path
            })

        # Quarta fase: controllo dei grafi per file
        for zip_path, omids in omids_by_file.items():
            # Carica il grafo una volta sola per tutti gli OMID nel file
            data_graph = ConjunctiveGraph()
            with zipfile.ZipFile(zip_path, 'r') as z:
                json_files = [f for f in z.namelist() if f.endswith('.json')]
                if json_files:
                    with z.open(json_files[0]) as f:
                        data_graph.parse(data=f.read(), format='json-ld')
            
            # Carica il grafo di provenance se esiste
            prov_graph = None
            prov_path = find_prov_file(zip_path)
            if prov_path and os.path.exists(prov_path):
                prov_graph = ConjunctiveGraph()
                with zipfile.ZipFile(prov_path, 'r') as z:
                    json_files = [f for f in z.namelist() if f.endswith('.json')]
                    if json_files:
                        with z.open(json_files[0]) as f:
                            prov_graph.parse(data=f.read(), format='json-ld')
            
            # Controlla tutti gli OMID nel file
            for omid in omids:
                if omid in omid_results_cache:
                    data_found, prov_found = omid_results_cache[omid]
                else:
                    omid_uri = URIRef(omid)
                    data_found = any(data_graph.triples((omid_uri, None, None)))
                    
                    if prov_graph is not None:
                        prov_graph_uri = URIRef(f"{omid}/prov/")
                        prov_found = any(prov_graph.triples((None, None, None), prov_graph_uri))
                    else:
                        prov_found = False
                    
                    omid_results_cache[omid] = (data_found, prov_found)
                
                if data_found:
                    stats['data_graphs_found'] += 1
                else:
                    stats['data_graphs_missing'] += 1
                    
                if prov_found:
                    stats['prov_graphs_found'] += 1
                else:
                    stats['prov_graphs_missing'] += 1
                
                # Trova il contesto originale dell'OMID
                for row_num, col, identifier in row_identifiers:
                    id_key = f"{identifier['schema']}:{identifier['value']}"
                    if omid in identifier_cache.get(id_key, set()):
                        stats['processed_omids'][omid] = {
                            'row': row_num,
                            'column': col,
                            'identifier': id_key,
                            'file': file_path,
                            'data_found': data_found,
                            'prov_found': prov_found
                        }
                        break
                
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
    
    return stats

def main():
    parser = argparse.ArgumentParser(description="Check MetaProcess results by verifying input CSV identifiers")
    parser.add_argument("directory", help="Directory containing input CSV files")
    parser.add_argument("meta_config", help="Path to meta_config.yaml file")
    args = parser.parse_args()
    
    # Load meta_config.yaml
    with open(args.meta_config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Extract needed paths and endpoint from config
    base_output_dir = config['output_rdf_dir']
    output_rdf_dir = os.path.join(base_output_dir, 'rdf')
    endpoint_url = config['triplestore_url']
    
    # Verify rdf directory exists
    if not os.path.exists(output_rdf_dir):
        print(f"RDF directory not found at {output_rdf_dir}")
        return
    
    # Find all CSV files in the directory and subdirectories
    csv_files = []
    for root, _, files in os.walk(args.directory):
        csv_files.extend(
            os.path.join(root, f) for f in files if f.endswith('.csv')
        )
    
    if not csv_files:
        print(f"No CSV files found in {args.directory}")
        return
        
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Process files in parallel
    with Pool(cpu_count()) as pool:
        process_args = [(f, endpoint_url, output_rdf_dir) for f in csv_files]
        results = list(tqdm(
            pool.imap(process_csv_file, process_args),
            total=len(csv_files),
            desc="Processing CSV files"
        ))
    
    # Aggregate results
    total_rows = sum(r['total_rows'] for r in results)
    total_rows_with_ids = sum(r['rows_with_ids'] for r in results)
    total_identifiers = sum(r['total_identifiers'] for r in results)
    total_with_omids = sum(r['identifiers_with_omids'] for r in results)
    total_without_omids = sum(r['identifiers_without_omids'] for r in results)
    total_data_graphs_found = sum(r['data_graphs_found'] for r in results)
    total_data_graphs_missing = sum(r['data_graphs_missing'] for r in results)
    total_prov_graphs_found = sum(r['prov_graphs_found'] for r in results)
    total_prov_graphs_missing = sum(r['prov_graphs_missing'] for r in results)
    
    # Verifica identificatori con OMID multipli e senza OMID
    problematic_identifiers = {}  # identificatori con OMID multipli
    missing_omid_identifiers = {}  # identificatori senza OMID
    
    for result in results:
        for detail in result['identifiers_details']:
            id_key = f"{detail['schema']}:{detail['value']}"
            omids = set()
            
            # Cerca gli OMID associati a questo identificatore in tutti i risultati
            for r in results:
                for omid, omid_details in r['processed_omids'].items():
                    if omid_details['identifier'] == id_key:
                        omids.add(omid)
            
            if len(omids) > 1:
                # Identificatore con OMID multipli
                if id_key not in problematic_identifiers:
                    problematic_identifiers[id_key] = {
                        'omids': omids,
                        'occurrences': []
                    }
                problematic_identifiers[id_key]['occurrences'].append({
                    'file': detail['file'],
                    'row': detail['row_number'],
                    'column': detail['column']
                })
            elif len(omids) == 0:
                # Identificatore senza OMID
                if id_key not in missing_omid_identifiers:
                    missing_omid_identifiers[id_key] = []
                missing_omid_identifiers[id_key].append({
                    'file': detail['file'],
                    'row': detail['row_number'],
                    'column': detail['column']
                })

    # Print summary
    print("\nResults Summary:")
    print(f"Total rows processed: {total_rows}")
    print(f"Rows containing identifiers: {total_rows_with_ids}")
    print(f"Total identifiers found: {total_identifiers}")
    print(f"Identifiers with associated OMIDs: {total_with_omids} ({(total_with_omids/total_identifiers*100):.2f}%)")
    print(f"Identifiers without OMIDs: {total_without_omids} ({(total_without_omids/total_identifiers*100):.2f}%)")
    print(f"\nData Graphs:")
    print(f"  Found: {total_data_graphs_found}")
    print(f"  Missing: {total_data_graphs_missing}")
    print(f"\nProvenance Graphs:")
    print(f"  Found: {total_prov_graphs_found}")
    print(f"  Missing: {total_prov_graphs_missing}")
    
    # Report problemi
    if problematic_identifiers:
        print("\nWARNING: Found identifiers with multiple OMIDs:")
        for id_key, details in problematic_identifiers.items():
            print(f"\nIdentifier {id_key} is associated with {len(details['omids'])} different OMIDs:")
            print(f"  OMIDs: {', '.join(sorted(details['omids']))}")
            print("  Occurrences:")
            for occ in details['occurrences']:
                print(f"    - Row {occ['row']} in {occ['file']}, column {occ['column']}")
    
    # if missing_omid_identifiers:
    #     print("\nWARNING: Found identifiers without any OMID:")
    #     for id_key, occurrences in missing_omid_identifiers.items():
    #         print(f"\nIdentifier {id_key} has no associated OMID")
    #         print("  Occurrences:")
    #         for occ in occurrences:
    #             print(f"    - Row {occ['row']} in {occ['file']}, column {occ['column']}")

if __name__ == "__main__":
    main() 