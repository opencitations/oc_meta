import argparse
import csv
import os
import re
import zipfile
from datetime import datetime
from multiprocessing import Pool, cpu_count
from typing import Dict, List, Set

import yaml
from oc_meta.lib.master_of_regex import name_and_ids, semicolon_in_people_field
from oc_meta.lib.sparql_utils import safe_sparql_query_with_retry
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
        parts = identifier.split(':', 1)
        if len(parts) == 2:
            identifiers.append({
                'schema': parts[0].lower(),
                'value': parts[1]
            })
    return identifiers

def check_provenance_existence(omids: List[str], prov_endpoint_url: str) -> Dict[str, bool]:
    """
    Query provenance SPARQL endpoint to check if provenance exists for the given OMIDs
    Returns dict mapping OMID to boolean indicating if provenance exists
    """
    if not omids:
        return {}
    
    prov_results = {}
    
    for omid in omids:
        prov_results[omid] = False

    for i in range(0, len(omids), BATCH_SIZE):
        batch = omids[i:i + BATCH_SIZE]

        union_patterns = []
        for omid in batch:
            snapshot_uri = f"{omid}/prov/se/1"
            union_patterns.append(f"{{ <{snapshot_uri}> prov:specializationOf ?entity . BIND(<{omid}> AS ?omid) }}")

        union_query = "\n            UNION\n            ".join(union_patterns)

        sparql = SPARQLWrapper(prov_endpoint_url)
        query = f"""
        PREFIX prov: <http://www.w3.org/ns/prov#>

        SELECT DISTINCT ?omid
        WHERE {{
            {union_query}
        }}
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        try:
            results = safe_sparql_query_with_retry(sparql)

            for result in results["results"]["bindings"]:
                omid = result["omid"]["value"]
                prov_results[omid] = True

        except Exception as e:
            print(f"SPARQL query failed for provenance batch check: {str(e)}")
            pass
    
    return prov_results

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
            results = safe_sparql_query_with_retry(sparql)
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

def process_csv_file(args: tuple):
    """
    Process a single CSV file and check its identifiers
    Returns statistics about processed rows and found/missing OMIDs
    """
    csv_file, endpoint_url, prov_endpoint_url, rdf_dir, dir_split_number, items_per_file, zip_output_rdf, generate_rdf_files = args
    
    stats = {
        'total_rows': 0,
        'rows_with_ids': 0,
        'total_identifiers': 0,
        'identifiers_with_omids': 0,
        'identifiers_without_omids': 0,
        'omid_schema_identifiers': 0,
        'data_graphs_found': 0,
        'data_graphs_missing': 0,
        'prov_graphs_found': 0,
        'prov_graphs_missing': 0,
        'omids_with_provenance': 0,
        'omids_without_provenance': 0,
        'identifiers_details': [],
        'processed_omids': {}
    }
    
    identifier_cache = {}  # chiave: "schema:value", valore: set di OMID
    omid_results_cache = {}  # chiave: omid, valore: (data_found, prov_found)
    
    unique_identifiers = set()
    row_identifiers = []  # Lista di tuple (row_num, col, identifier)
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1):
                stats['total_rows'] += 1
                row_has_ids = False
                
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
                            if identifier['schema'].lower() != 'omid':
                                unique_identifiers.add(id_key)
                            else:
                                stats['omid_schema_identifiers'] += 1
                            row_identifiers.append((row_num, col, identifier))
                
                if row_has_ids:
                    stats['rows_with_ids'] += 1

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
            
            if identifier['schema'].lower() == 'omid':
                pass
            else:
                omids = identifier_cache.get(id_key, set())
                
                if omids:
                    stats['identifiers_with_omids'] += 1
                    all_omids.update(omids)
                    
                    # Raggruppa OMID per file
                    for omid in omids:
                        if omid not in omid_results_cache:  # Skip se già controllato
                            if generate_rdf_files:
                                zip_path = find_file(rdf_dir, dir_split_number, items_per_file, omid, zip_output_rdf)
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
                'file': csv_file
            })

        # Quarta fase: controllo dei grafi per file
        for zip_path, omids in omids_by_file.items():
            data_content = None
            with zipfile.ZipFile(zip_path, 'r') as z:
                json_files = [f for f in z.namelist() if f.endswith('.json')]
                if json_files:
                    with z.open(json_files[0]) as f:
                        data_content = f.read().decode('utf-8')

            prov_content = None
            prov_path = find_prov_file(zip_path)
            if prov_path and os.path.exists(prov_path):
                with zipfile.ZipFile(prov_path, 'r') as z:
                    json_files = [f for f in z.namelist() if f.endswith('.json')]
                    if json_files:
                        with z.open(json_files[0]) as f:
                            prov_content = f.read().decode('utf-8')

            for omid in omids:
                if omid in omid_results_cache:
                    data_found, prov_found = omid_results_cache[omid]
                else:
                    data_found = data_content is not None and omid in data_content

                    if prov_content is not None:
                        prov_found = omid in prov_content
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
                            'file': csv_file,
                            'data_found': data_found,
                            'prov_found': prov_found
                        }
                        break
                        
        for row_num, col, identifier in row_identifiers:
            if identifier['schema'].lower() == 'omid':
                omid = identifier['value']
                if omid.startswith('http'):
                    all_omids.add(omid)
        
        all_omids.update(stats['processed_omids'].keys())
        
        prov_results = check_provenance_existence(list(all_omids), prov_endpoint_url)
        
        for omid, has_prov in prov_results.items():
            if has_prov:
                stats['omids_with_provenance'] += 1
            else:
                stats['omids_without_provenance'] += 1
                
            if omid in stats['processed_omids']:
                stats['processed_omids'][omid]['triplestore_prov_found'] = has_prov
                
    except Exception as e:
        print(f"Error processing {csv_file}: {str(e)}")
    
    return stats

def generate_results_output(
    total_rows: int,
    total_rows_with_ids: int,
    total_identifiers: int,
    total_omid_schema: int,
    total_with_omids: int,
    total_without_omids: int,
    total_data_graphs_found: int,
    total_data_graphs_missing: int,
    total_prov_graphs_found: int,
    total_prov_graphs_missing: int,
    total_omids_with_provenance: int,
    total_omids_without_provenance: int,
    total_found_omids: int,
    problematic_identifiers: dict,
    omids_without_prov: dict,
    missing_omid_identifiers: dict,
    generate_rdf_files: bool,
    include_header: bool = False
) -> List[str]:
    """Generate output lines for results (used both for console and file)"""
    lines = []

    if include_header:
        lines.append("=" * 80)
        lines.append("CHECK RESULTS REPORT")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 80)
        lines.append("")

    lines.append("Results Summary:")
    lines.append(f"Total rows processed: {total_rows}")
    lines.append(f"Rows containing identifiers: {total_rows_with_ids}")
    lines.append(f"Total identifiers found: {total_identifiers}")
    lines.append(f"Identifiers with 'omid' schema (skipped checking): {total_omid_schema}")

    non_omid_identifiers = total_identifiers - total_omid_schema
    if non_omid_identifiers > 0:
        lines.append(f"Identifiers with associated OMIDs: {total_with_omids} ({(total_with_omids/non_omid_identifiers*100):.2f}%)")
        lines.append(f"Identifiers without OMIDs: {total_without_omids} ({(total_without_omids/non_omid_identifiers*100):.2f}%)")
    else:
        lines.append("No non-omid identifiers found to check for OMID associations.")

    if generate_rdf_files:
        lines.append("")
        lines.append("Data Graphs:")
        lines.append(f"  Found: {total_data_graphs_found}")
        lines.append(f"  Missing: {total_data_graphs_missing}")
        lines.append("")
        lines.append("Provenance Graphs:")
        lines.append(f"  Found: {total_prov_graphs_found}")
        lines.append(f"  Missing: {total_prov_graphs_missing}")
    else:
        lines.append("")
        lines.append("RDF file generation is disabled. File checks were skipped.")

    lines.append("")
    lines.append("Provenance in Triplestore:")
    total_omids_checked = total_omids_with_provenance + total_omids_without_provenance
    if total_omids_checked > 0:
        lines.append(f"  OMIDs with provenance: {total_omids_with_provenance} ({(total_omids_with_provenance/total_omids_checked*100):.2f}%)")
        lines.append(f"  OMIDs without provenance: {total_omids_without_provenance} ({(total_omids_without_provenance/total_omids_checked*100):.2f}%)")
    else:
        lines.append("  No OMIDs found to check for provenance.")

    if problematic_identifiers:
        lines.append("")
        lines.append("=" * 80)
        lines.append("WARNING: Found identifiers with multiple OMIDs:")
        lines.append("=" * 80)
        for id_key, details in problematic_identifiers.items():
            lines.append(f"\nIdentifier {id_key} is associated with {len(details['omids'])} different OMIDs:")
            lines.append(f"  OMIDs: {', '.join(sorted(details['omids']))}")
            lines.append("  Occurrences:")
            for occ in details['occurrences']:
                lines.append(f"    - Row {occ['row']} in {occ['file']}, column {occ['column']}")

    if omids_without_prov:
        lines.append("")
        lines.append("=" * 80)
        lines.append("WARNING: Found OMIDs without provenance in the triplestore:")
        lines.append("=" * 80)
        for omid, occurrences in omids_without_prov.items():
            lines.append(f"\nOMID {omid} has no associated provenance")
            lines.append("  Referenced by:")
            for occ in occurrences:
                lines.append(f"    - Identifier {occ['identifier']} in {occ['file']}, row {occ['row']}, column {occ['column']}")

    if missing_omid_identifiers:
        lines.append("")
        lines.append("=" * 80)
        lines.append("WARNING: Found identifiers without any OMID:")
        lines.append("=" * 80)
        for id_key, occurrences in missing_omid_identifiers.items():
            lines.append(f"\nIdentifier {id_key} has no associated OMID")
            lines.append("  Occurrences:")
            for occ in occurrences:
                lines.append(f"    - Row {occ['row']} in {occ['file']}, column {occ['column']}")

    return lines

def main():
    parser = argparse.ArgumentParser(description="Check MetaProcess results by verifying input CSV identifiers")
    parser.add_argument("meta_config", help="Path to meta_config.yaml file")
    parser.add_argument("--output", help="Output file path. If specified, results are written to file instead of console")
    args = parser.parse_args()

    with open(args.meta_config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    input_csv_dir = config['input_csv_dir']
    base_output_dir = config['output_rdf_dir']
    output_rdf_dir = os.path.join(base_output_dir, 'rdf')
    endpoint_url = config['triplestore_url']
    prov_endpoint_url = config['provenance_triplestore_url']
    generate_rdf_files = config.get('generate_rdf_files', True)
    
    if generate_rdf_files and not os.path.exists(output_rdf_dir):
        print(f"RDF directory not found at {output_rdf_dir}")
        return
    
    csv_files = []
    for root, _, files in os.walk(input_csv_dir):
        csv_files.extend(
            os.path.join(root, f) for f in files if f.endswith('.csv')
        )

    if not csv_files:
        print(f"No CSV files found in {input_csv_dir}")
        return
        
    print(f"Found {len(csv_files)} CSV files to process")
    
    with Pool(cpu_count()) as pool:
        process_args = [(f, endpoint_url, prov_endpoint_url, output_rdf_dir, config['dir_split_number'], config['items_per_file'], config['zip_output_rdf'], generate_rdf_files) for f in csv_files]
        results = list(tqdm(
            pool.imap(process_csv_file, process_args),
            total=len(csv_files),
            desc="Processing CSV files"
        ))
    
    total_rows = sum(r['total_rows'] for r in results)
    total_rows_with_ids = sum(r['rows_with_ids'] for r in results)
    total_identifiers = sum(r['total_identifiers'] for r in results)
    total_with_omids = sum(r['identifiers_with_omids'] for r in results)
    total_without_omids = sum(r['identifiers_without_omids'] for r in results)
    total_omid_schema = sum(r['omid_schema_identifiers'] for r in results)
    total_data_graphs_found = sum(r['data_graphs_found'] for r in results)
    total_data_graphs_missing = sum(r['data_graphs_missing'] for r in results)
    total_prov_graphs_found = sum(r['prov_graphs_found'] for r in results)
    total_prov_graphs_missing = sum(r['prov_graphs_missing'] for r in results)
    total_omids_with_provenance = sum(r['omids_with_provenance'] for r in results)
    total_omids_without_provenance = sum(r['omids_without_provenance'] for r in results)

    id_key_to_omids = {}
    for r in tqdm(results, desc="Creating lookup dictionary"):
        for omid, omid_details in r['processed_omids'].items():
            id_key = omid_details['identifier']
            if id_key not in id_key_to_omids:
                id_key_to_omids[id_key] = set()
            id_key_to_omids[id_key].add(omid)

    problematic_identifiers = {}  # identificatori con OMID multipli
    missing_omid_identifiers = {}  # identificatori senza OMID

    for result in tqdm(results, desc="Checking for problematic identifiers"):
        for detail in result['identifiers_details']:
            id_key = f"{detail['schema']}:{detail['value']}"
            omids = id_key_to_omids.get(id_key, set())

            if len(omids) > 1:
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
            # We only want to add identifiers to missing_omid_identifiers if:
            # 1. They have no omids associated according to the SPARQL check (len(omids) == 0)
            # 2. They're not OMID schema identifiers (already excluded)
            # 3. The identifier details show has_omid=False (wasn't marked as found during processing)
            elif len(omids) == 0 and detail['schema'].lower() != 'omid' and not detail['has_omid']:
                if id_key not in missing_omid_identifiers:
                    missing_omid_identifiers[id_key] = []
                missing_omid_identifiers[id_key].append({
                    'file': detail['file'],
                    'row': detail['row_number'],
                    'column': detail['column']
                })

    omids_without_prov_set = set()
    for result in tqdm(results, desc="Building provenance lookup set"):
        for omid, details in result['processed_omids'].items():
            if not details.get('triplestore_prov_found', False):
                omids_without_prov_set.add(omid)

    omids_without_prov = {}
    for result in tqdm(results, desc="Collecting OMIDs without provenance"):
        for omid, details in result['processed_omids'].items():
            if not details.get('triplestore_prov_found', False):
                if omid not in omids_without_prov:
                    omids_without_prov[omid] = []
                omids_without_prov[omid].append({
                    'file': details.get('file', 'unknown'),
                    'row': details.get('row', 'unknown'),
                    'column': details.get('column', 'unknown'),
                    'identifier': details.get('identifier', 'unknown')
                })

        for detail in result['identifiers_details']:
            if detail['schema'].lower() == 'omid':
                omid = detail['value']
                if omid.startswith('http') and omid in omids_without_prov_set:
                    if omid not in omids_without_prov:
                        omids_without_prov[omid] = []
                    omids_without_prov[omid].append({
                        'file': detail.get('file', 'unknown'),
                        'row': detail.get('row_number', 'unknown'),
                        'column': detail.get('column', 'unknown'),
                        'identifier': f"omid:{omid}"
                    })

    total_found_omids = total_with_omids + total_omid_schema

    output_lines = generate_results_output(
        total_rows=total_rows,
        total_rows_with_ids=total_rows_with_ids,
        total_identifiers=total_identifiers,
        total_omid_schema=total_omid_schema,
        total_with_omids=total_with_omids,
        total_without_omids=total_without_omids,
        total_data_graphs_found=total_data_graphs_found,
        total_data_graphs_missing=total_data_graphs_missing,
        total_prov_graphs_found=total_prov_graphs_found,
        total_prov_graphs_missing=total_prov_graphs_missing,
        total_omids_with_provenance=total_omids_with_provenance,
        total_omids_without_provenance=total_omids_without_provenance,
        total_found_omids=total_found_omids,
        problematic_identifiers=problematic_identifiers,
        omids_without_prov=omids_without_prov,
        missing_omid_identifiers=missing_omid_identifiers,
        generate_rdf_files=generate_rdf_files,
        include_header=bool(args.output)
    )

    if args.output:
        output_dir = os.path.dirname(args.output) or '.'
        os.makedirs(output_dir, exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"Results written to: {args.output}")
    else:
        print('\n' + '\n'.join(output_lines))

if __name__ == "__main__":
    main() 