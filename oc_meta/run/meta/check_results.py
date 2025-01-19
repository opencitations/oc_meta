import argparse
import csv
import os
import re
import zipfile
from multiprocessing import Pool, cpu_count, Manager
from typing import List, Dict, Set
from rdflib import ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm

def parse_identifiers(id_string: str) -> List[Dict[str, str]]:
    """
    Parse space-separated identifiers in the format schema:value
    Returns a list of dicts with 'schema' and 'value' keys
    """
    if not id_string or id_string.isspace():
        return []
        
    identifiers = []
    for identifier in id_string.strip().split():
        try:
            schema, value = identifier.split(':', 1)
            identifiers.append({
                'schema': schema.lower(),
                'value': value
            })
        except ValueError:
            continue
    return identifiers

def check_omids_existence(identifiers: List[Dict[str, str]], endpoint_url: str) -> Set[str]:
    """
    Query SPARQL endpoint to find OMIDs for a single identifier
    Returns set of found OMIDs
    """
    if not identifiers or len(identifiers) != 1:
        return set()
    
    identifier = identifiers[0]
    sparql = SPARQLWrapper(endpoint_url)
    
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
        results = sparql.query().convert()
        omids = set()
        for result in results["results"]["bindings"]:
            omid = result["omid"]["value"]
            omids.add(omid)
        return omids
    except Exception as e:
        print(f"SPARQL query failed for identifier {identifier}: {str(e)}")
        return set()

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

def load_graph_from_zip(zip_path: str) -> ConjunctiveGraph|None:
    """Load RDF graph from a ZIP file"""
    try:
        graph = ConjunctiveGraph()
        with zipfile.ZipFile(zip_path, 'r') as z:
            json_files = [f for f in z.namelist() if f.endswith('.json')]
            if not json_files:
                return None
            with z.open(json_files[0]) as f:
                graph.parse(data=f.read(), format='json-ld')
        return graph
    except Exception as e:
        print(f"Error loading graph from {zip_path}: {str(e)}")
        return None

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
        'identifiers_details': [],  # To store detailed info about each identifier
        'graphs_found': 0,
        'graphs_missing': 0,
        'processed_omids': {}  # Track which OMID was found and in which row
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats['total_rows'] += 1
                
                id_column = row.get('id', '').strip()
                if not id_column:
                    continue
                    
                stats['rows_with_ids'] += 1
                identifiers = parse_identifiers(id_column)
                stats['total_identifiers'] += len(identifiers)
                
                # Check each identifier individually
                for identifier in identifiers:
                    omids = check_omids_existence([identifier], endpoint_url)
                    has_omid = len(omids) > 0
                    
                    if has_omid:
                        if len(omids) > 1:
                            print(f"Warning: Identifier {identifier['schema']}:{identifier['value']} "
                                  f"is associated with multiple OMIDs: {omids}")
                        stats['identifiers_with_omids'] += 1
                        # Try to load the graph for each OMID
                        for omid in omids:
                            zip_path = find_file(root_dir, 10000, 1000, omid, True)
                            if zip_path and os.path.exists(zip_path):
                                graph = load_graph_from_zip(zip_path)
                                if graph is not None:
                                    stats['graphs_found'] += 1
                                    stats['processed_omids'][omid] = {
                                        'row': stats['total_rows'],
                                        'identifier': f"{identifier['schema']}:{identifier['value']}",
                                        'file': file_path
                                    }
                                else:
                                    stats['graphs_missing'] += 1
                            else:
                                stats['graphs_missing'] += 1
                    else:
                        stats['identifiers_without_omids'] += 1
                    
                    stats['identifiers_details'].append({
                        'schema': identifier['schema'],
                        'value': identifier['value'],
                        'has_omid': has_omid,
                        'row_number': stats['total_rows']
                    })
                
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        
    return stats

def main():
    parser = argparse.ArgumentParser(description="Check MetaProcess results by verifying input CSV identifiers")
    parser.add_argument("directory", help="Directory containing input CSV files")
    parser.add_argument("--root", required=True,
                       help="Root directory containing the ZIP files with JSON-LD data")
    parser.add_argument("--endpoint", required=True, help="SPARQL endpoint URL")
    parser.add_argument("--show-missing", action="store_true", 
                       help="Show details of identifiers without associated OMIDs")
    args = parser.parse_args()
    
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
    
    # Process files in parallel (senza shared data)
    with Pool(cpu_count()) as pool:
        process_args = [(f, args.endpoint, args.root) for f in csv_files]
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
    total_graphs_found = sum(r.get('graphs_found', 0) for r in results)
    total_graphs_missing = sum(r.get('graphs_missing', 0) for r in results)
    
    # Collect all OMID occurrences
    all_omids = {}
    for result in results:
        for omid, details in result['processed_omids'].items():
            if omid not in all_omids:
                all_omids[omid] = []
            all_omids[omid].append(details)
    
    # Find duplicates
    duplicates = {omid: occurrences for omid, occurrences in all_omids.items() if len(occurrences) > 1}
    
    # Print summary
    print("\nResults Summary:")
    print(f"Total rows processed: {total_rows}")
    print(f"Rows containing identifiers: {total_rows_with_ids}")
    print(f"Total identifiers found: {total_identifiers}")
    print(f"Identifiers with associated OMIDs: {total_with_omids} ({(total_with_omids/total_identifiers*100):.2f}%)")
    print(f"Identifiers without OMIDs: {total_without_omids} ({(total_without_omids/total_identifiers*100):.2f}%)")
    print(f"Graphs found in ZIP files: {total_graphs_found}")
    print(f"Graphs missing from ZIP files: {total_graphs_missing}")
    print(f"Unique OMIDs found: {len(all_omids)}")
    
    if duplicates:
        print("\nOMIDs found multiple times:")
        for omid, occurrences in duplicates.items():
            print(f"\nOMID {omid} appeared {len(occurrences)} times:")
            for occurrence in occurrences:
                print(f"  Row {occurrence['row']} in {occurrence['file']}")
                print(f"  Identifier: {occurrence['identifier']}")

if __name__ == "__main__":
    main() 