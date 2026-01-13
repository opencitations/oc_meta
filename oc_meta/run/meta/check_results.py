import argparse
import csv
import os
import re
import time
import zipfile
from datetime import datetime
from typing import Callable, Dict, List, Set, TypeVar

import yaml
from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.master_of_regex import name_and_ids, semicolon_in_people_field
from rich.progress import Progress, BarColumn, TaskProgressColumn, TimeRemainingColumn, TextColumn
from sparqlite import SPARQLClient
from sparqlite.exceptions import EndpointError

T = TypeVar('T')

BATCH_SIZE = 10
MAX_RETRIES = 10
RETRY_BACKOFF = 2


def retry_on_error(func: Callable[[], T]) -> T:
    """Retry a function on EndpointError (including 4xx errors from Virtuoso)."""
    for attempt in range(MAX_RETRIES):
        try:
            return func()
        except EndpointError:
            if attempt == MAX_RETRIES - 1:
                raise
            wait_time = RETRY_BACKOFF ** attempt
            time.sleep(wait_time)
    raise RuntimeError("Unreachable")


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
            value = Cleaner(parts[1]).normalize_hyphens()
            identifiers.append({
                'schema': parts[0].lower(),
                'value': value
            })
    return identifiers

def check_provenance_existence(omids: List[str], prov_endpoint_url: str) -> Dict[str, bool]:
    """
    Query provenance SPARQL endpoint to check if provenance exists for the given OMIDs.
    Uses ASK queries on the snapshot URI for better performance.
    Returns dict mapping OMID to boolean indicating if provenance exists.
    """
    if not omids:
        return {}

    prov_results = {omid: False for omid in omids}

    with SPARQLClient(prov_endpoint_url, max_retries=10, backoff_factor=2, timeout=3600) as client:
        for omid in omids:
            snapshot_uri = f"{omid}/prov/se/1"
            query = f"ASK {{ <{snapshot_uri}> <http://www.w3.org/ns/prov#specializationOf> ?o }}"
            prov_results[omid] = retry_on_error(lambda q=query: client.ask(q))

    return prov_results

def check_omids_existence(identifiers: List[Dict[str, str]], endpoint_url: str) -> Dict[str, Set[str]]:
    """
    Query SPARQL endpoint to find OMIDs for a single identifier
    Returns dict mapping identifier keys to sets of found OMIDs
    """
    if not identifiers:
        return {}

    found_omids = {}

    with SPARQLClient(endpoint_url, max_retries=10, backoff_factor=2, timeout=3600) as client:
        for identifier in identifiers:
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

            results = retry_on_error(lambda q=query: client.query(q))
            omids = set()

            for result in results["results"]["bindings"]:
                omid = result["omid"]["value"]
                omids.add(omid)

            found_omids[id_key] = omids

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

def process_csv_file(args: tuple, progress=None, task_id=None):
    """
    Process a single CSV file and check its identifiers
    Returns statistics about processed rows and found/missing OMIDs
    """
    csv_file, endpoint_url, prov_endpoint_url, rdf_dir, dir_split_number, items_per_file, zip_output_rdf, generate_rdf_files = args

    def update_phase(phase: str):
        if progress and task_id is not None:
            progress.update(task_id, detail=phase)

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

    update_phase("Phase 1/5: Reading CSV")
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

    update_phase(f"Phase 2/5: Querying DB for {len(all_identifiers)} identifiers")
    for i in range(0, len(all_identifiers), BATCH_SIZE):
        batch = all_identifiers[i:i + BATCH_SIZE]
        batch_results = check_omids_existence(batch, endpoint_url)
        identifier_cache.update(batch_results)

    # Terza fase: mappatura OMID -> file ZIP
    update_phase("Phase 3/5: Mapping OMIDs to files")
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
                all_omids.add(next(iter(omids)))  # Only one OMID per identifier for provenance check

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
    update_phase(f"Phase 4/5: Checking {len(omids_by_file)} RDF files")
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

    update_phase(f"Phase 5/5: Checking provenance for {len(all_omids)} OMIDs")
    prov_results = check_provenance_existence(list(all_omids), prov_endpoint_url)

    for omid, has_prov in prov_results.items():
        if has_prov:
            stats['omids_with_provenance'] += 1
        else:
            stats['omids_without_provenance'] += 1

        if omid in stats['processed_omids']:
            stats['processed_omids'][omid]['triplestore_prov_found'] = has_prov

    return stats

def write_header(f):
    """Write report header to file"""
    f.write("=" * 80 + "\n")
    f.write("CHECK RESULTS REPORT\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 80 + "\n\n")
    f.flush()


def write_file_report(f, csv_file: str, stats: dict, generate_rdf_files: bool):
    """Write report section for a single CSV file, including any problems found"""
    filename = os.path.basename(csv_file)
    f.write(f"--- File: {filename} ---\n")

    # Basic stats
    non_omid = stats['total_identifiers'] - stats['omid_schema_identifiers']
    f.write(f"Rows: {stats['total_rows']}, With IDs: {stats['rows_with_ids']}, ")
    f.write(f"Identifiers: {stats['total_identifiers']} (omid schema: {stats['omid_schema_identifiers']})\n")

    if non_omid > 0:
        f.write(f"With OMID: {stats['identifiers_with_omids']}, Without OMID: {stats['identifiers_without_omids']}\n")

    if generate_rdf_files:
        f.write(f"Data graphs - Found: {stats['data_graphs_found']}, Missing: {stats['data_graphs_missing']}\n")
        f.write(f"Prov graphs - Found: {stats['prov_graphs_found']}, Missing: {stats['prov_graphs_missing']}\n")

    f.write(f"Provenance in DB - With: {stats['omids_with_provenance']}, Without: {stats['omids_without_provenance']}\n")

    # Problems for this file
    problems_found = False

    # Identifiers without OMID
    missing_omids = [d for d in stats['identifiers_details']
                    if not d['has_omid'] and d['schema'].lower() != 'omid']
    if missing_omids:
        if not problems_found:
            f.write("\nProblems in this file:\n")
            problems_found = True
        for detail in missing_omids:
            f.write(f"  - Identifier {detail['schema']}:{detail['value']} has no OMID ")
            f.write(f"(row {detail['row_number']}, column {detail['column']})\n")

    # OMIDs without provenance (only those that were actually checked)
    omids_no_prov = [(omid, details) for omid, details in stats['processed_omids'].items()
                     if 'triplestore_prov_found' in details and not details['triplestore_prov_found']]
    if omids_no_prov:
        if not problems_found:
            f.write("\nProblems in this file:\n")
            problems_found = True
        for omid, details in omids_no_prov:
            f.write(f"  - OMID {omid} has no provenance ")
            f.write(f"(row {details['row']}, identifier {details['identifier']})\n")

    f.write("\n")
    f.flush()


def write_aggregated_summary(
    f,
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
    problematic_identifiers: dict,
    generate_rdf_files: bool
):
    """Write aggregated summary at the end of the report"""
    f.write("=" * 80 + "\n")
    f.write("AGGREGATED SUMMARY\n")
    f.write("=" * 80 + "\n\n")

    f.write(f"Total rows processed: {total_rows}\n")
    f.write(f"Rows containing identifiers: {total_rows_with_ids}\n")
    f.write(f"Total identifiers found: {total_identifiers}\n")
    f.write(f"Identifiers with 'omid' schema (skipped checking): {total_omid_schema}\n")

    non_omid_identifiers = total_identifiers - total_omid_schema
    if non_omid_identifiers > 0:
        f.write(f"Identifiers with associated OMIDs: {total_with_omids} ({(total_with_omids/non_omid_identifiers*100):.2f}%)\n")
        f.write(f"Identifiers without OMIDs: {total_without_omids} ({(total_without_omids/non_omid_identifiers*100):.2f}%)\n")
    else:
        f.write("No non-omid identifiers found to check for OMID associations.\n")

    if generate_rdf_files:
        f.write(f"\nData Graphs - Found: {total_data_graphs_found}, Missing: {total_data_graphs_missing}\n")
        f.write(f"Provenance Graphs - Found: {total_prov_graphs_found}, Missing: {total_prov_graphs_missing}\n")
    else:
        f.write("\nRDF file generation is disabled. File checks were skipped.\n")

    total_omids_checked = total_omids_with_provenance + total_omids_without_provenance
    if total_omids_checked > 0:
        f.write(f"\nProvenance in Triplestore:\n")
        f.write(f"  OMIDs with provenance: {total_omids_with_provenance} ({(total_omids_with_provenance/total_omids_checked*100):.2f}%)\n")
        f.write(f"  OMIDs without provenance: {total_omids_without_provenance} ({(total_omids_without_provenance/total_omids_checked*100):.2f}%)\n")
    else:
        f.write("\nNo OMIDs found to check for provenance.\n")

    # Cross-file problems: identifiers with multiple OMIDs
    if problematic_identifiers:
        f.write("\n" + "=" * 80 + "\n")
        f.write("WARNING: Found identifiers with multiple OMIDs (cross-file issue):\n")
        f.write("=" * 80 + "\n")
        for id_key, details in problematic_identifiers.items():
            f.write(f"\nIdentifier {id_key} is associated with {len(details['omids'])} different OMIDs:\n")
            f.write(f"  OMIDs: {', '.join(sorted(details['omids']))}\n")
            f.write("  Occurrences:\n")
            for occ in details['occurrences']:
                f.write(f"    - Row {occ['row']} in {occ['file']}, column {occ['column']}\n")

    f.flush()

def main():
    parser = argparse.ArgumentParser(description="Check MetaProcess results by verifying input CSV identifiers")
    parser.add_argument("meta_config", help="Path to meta_config.yaml file")
    parser.add_argument("output", help="Output file path for results")
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

    # Prepare output file
    output_dir = os.path.dirname(args.output) or '.'
    os.makedirs(output_dir, exist_ok=True)

    # Aggregation variables
    total_rows = 0
    total_rows_with_ids = 0
    total_identifiers = 0
    total_with_omids = 0
    total_without_omids = 0
    total_omid_schema = 0
    total_data_graphs_found = 0
    total_data_graphs_missing = 0
    total_prov_graphs_found = 0
    total_prov_graphs_missing = 0
    total_omids_with_provenance = 0
    total_omids_without_provenance = 0
    id_key_to_omids = {}
    all_results = []

    with open(args.output, 'w', encoding='utf-8') as output_file:
        write_header(output_file)

        process_args = [(f, endpoint_url, prov_endpoint_url, output_rdf_dir, config['dir_split_number'], config['items_per_file'], config['zip_output_rdf'], generate_rdf_files) for f in csv_files]

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TextColumn("[cyan]{task.fields[current_file]}"),
            TextColumn("[yellow]{task.fields[detail]}"),
        ) as progress:
            task = progress.add_task("Processing CSV files", total=len(csv_files), current_file="", detail="")
            for idx, proc_args in enumerate(process_args):
                current_file = os.path.basename(csv_files[idx])
                progress.update(task, current_file=f"[{idx+1}/{len(csv_files)}] {current_file}")

                result = process_csv_file(proc_args, progress=progress, task_id=task)

                # Write file report immediately
                write_file_report(output_file, csv_files[idx], result, generate_rdf_files)

                # Accumulate for aggregation
                total_rows += result['total_rows']
                total_rows_with_ids += result['rows_with_ids']
                total_identifiers += result['total_identifiers']
                total_with_omids += result['identifiers_with_omids']
                total_without_omids += result['identifiers_without_omids']
                total_omid_schema += result['omid_schema_identifiers']
                total_data_graphs_found += result['data_graphs_found']
                total_data_graphs_missing += result['data_graphs_missing']
                total_prov_graphs_found += result['prov_graphs_found']
                total_prov_graphs_missing += result['prov_graphs_missing']
                total_omids_with_provenance += result['omids_with_provenance']
                total_omids_without_provenance += result['omids_without_provenance']

                # Build cross-file lookup
                for omid, omid_details in result['processed_omids'].items():
                    id_key = omid_details['identifier']
                    if id_key not in id_key_to_omids:
                        id_key_to_omids[id_key] = set()
                    id_key_to_omids[id_key].add(omid)

                all_results.append(result)
                progress.advance(task)

        # Find cross-file problematic identifiers (same identifier with multiple OMIDs)
        problematic_identifiers = {}
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TextColumn("[cyan]{task.fields[detail]}"),
        ) as progress:
            task = progress.add_task("Checking cross-file issues", total=len(all_results), detail="")
            for result in all_results:
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
                progress.advance(task)
            progress.update(task, detail=f"Found {len(problematic_identifiers)} identifiers with multiple OMIDs")

        # Write aggregated summary
        write_aggregated_summary(
            output_file,
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
            problematic_identifiers=problematic_identifiers,
            generate_rdf_files=generate_rdf_files
        )

    print(f"Results written to: {args.output}")

if __name__ == "__main__":
    main() 