# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import os
import re
import sys
import zipfile
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Set

import orjson
import yaml
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn
from rich_argparse import RichHelpFormatter
from sparqlite import SPARQLClient

from oc_meta.constants import QLEVER_BATCH_SIZE, QLEVER_MAX_WORKERS, QLEVER_QUERIES_PER_GROUP
from oc_meta.lib.cleaner import normalize_hyphens
from oc_meta.lib.console import EMATimeRemainingColumn, console
from oc_meta.lib.file_manager import collect_files, get_csv_data
from oc_meta.lib.master_of_regex import RE_NAME_AND_IDS, RE_SEMICOLON_IN_PEOPLE_FIELD

MAX_RETRIES = 10
RETRY_BACKOFF = 2
DATACITE_PREFIX = "http://purl.org/spar/datacite/"


@dataclass
class FileStats:
    file: str
    total_rows: int = 0
    rows_with_ids: int = 0
    total_identifiers: int = 0
    omid_schema_identifiers: int = 0
    identifiers_with_omids: int = 0
    identifiers_without_omids: int = 0
    data_graphs_found: int = 0
    data_graphs_missing: int = 0
    prov_graphs_found: int = 0
    prov_graphs_missing: int = 0
    omids_with_provenance: int = 0
    omids_without_provenance: int = 0
    identifiers_details: list = field(default_factory=list)
    processed_omids: dict = field(default_factory=dict)


def _execute_sparql_queries(args: tuple) -> list:
    ts_url, queries = args
    results = []
    with SPARQLClient(ts_url, max_retries=MAX_RETRIES, backoff_factor=RETRY_BACKOFF, timeout=3600) as client:
        for query in queries:
            result = client.query(query)
            results.append(result['results']['bindings'] if result else [])
    return results


def _run_queries_parallel(
    endpoint_url: str,
    batch_queries: list[str],
    batch_sizes: list[int],
    workers: int = QLEVER_MAX_WORKERS,
    progress_callback: Callable[[int], None] | None = None,
) -> list[list]:
    if not batch_queries:
        return []

    all_bindings: list[list] = []

    if len(batch_queries) > 1 and workers > 1:
        grouped_queries = []
        grouped_sizes = []
        for i in range(0, len(batch_queries), QLEVER_QUERIES_PER_GROUP):
            grouped_queries.append((endpoint_url, batch_queries[i:i + QLEVER_QUERIES_PER_GROUP]))
            grouped_sizes.append(sum(batch_sizes[i:i + QLEVER_QUERIES_PER_GROUP]))

        with ProcessPoolExecutor(
            max_workers=min(len(grouped_queries), workers),
            mp_context=multiprocessing.get_context('forkserver')
        ) as executor:
            for idx, result in enumerate(executor.map(_execute_sparql_queries, grouped_queries)):
                all_bindings.extend(result)
                if progress_callback:
                    progress_callback(grouped_sizes[idx])
    else:
        results = _execute_sparql_queries((endpoint_url, batch_queries))
        all_bindings.extend(results)
        if progress_callback:
            progress_callback(sum(batch_sizes))

    return all_bindings


def parse_identifiers(id_string: str | None) -> List[Dict[str, str]]:
    if not id_string or id_string.isspace():
        return []

    identifiers = []
    for identifier in id_string.strip().split():
        parts = identifier.split(':', 1)
        if len(parts) == 2:
            value = normalize_hyphens(parts[1])
            identifiers.append({
                'schema': parts[0].lower(),
                'value': value
            })
    return identifiers

def check_provenance_existence(omids: List[str], prov_endpoint_url: str, workers: int = QLEVER_MAX_WORKERS, progress_callback: Callable[[int], None] | None = None) -> Dict[str, bool]:
    if not omids:
        return {}

    prov_results = {omid: False for omid in omids}

    batch_queries = []
    batch_sizes = []
    for i in range(0, len(omids), QLEVER_BATCH_SIZE):
        batch = omids[i:i + QLEVER_BATCH_SIZE]
        values_entries = " ".join(f"<{omid}/prov/se/1>" for omid in batch)
        query = f"""
        SELECT ?snapshot WHERE {{
            VALUES ?snapshot {{ {values_entries} }}
            ?snapshot <http://www.w3.org/ns/prov#specializationOf> ?o .
        }}
        """
        batch_queries.append(query)
        batch_sizes.append(len(batch))

    all_bindings = _run_queries_parallel(prov_endpoint_url, batch_queries, batch_sizes, workers, progress_callback)

    for bindings in all_bindings:
        for result in bindings:
            snapshot_uri = result["snapshot"]["value"]
            omid = snapshot_uri.rsplit("/prov/se/1", 1)[0]
            prov_results[omid] = True

    return prov_results

def check_omids_existence(identifiers: List[Dict[str, str]], endpoint_url: str, workers: int = QLEVER_MAX_WORKERS, progress_callback: Callable[[int], None] | None = None) -> Dict[str, Set[str]]:
    if not identifiers:
        return {}

    found_omids: Dict[str, Set[str]] = {}

    batch_queries = []
    batch_sizes = []
    for i in range(0, len(identifiers), QLEVER_BATCH_SIZE):
        batch = identifiers[i:i + QLEVER_BATCH_SIZE]

        values_entries = []
        for identifier in batch:
            escaped_value = identifier['value'].replace('\\', '\\\\').replace('"', '\\"')
            values_entries.append(f'("{escaped_value}"^^xsd:string datacite:{identifier["schema"]})')

        query = f"""
        PREFIX datacite: <http://purl.org/spar/datacite/>
        PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?val ?scheme ?omid
        WHERE {{
            VALUES (?val ?scheme) {{ {" ".join(values_entries)} }}
            ?omid literal:hasLiteralValue ?val ;
                  datacite:usesIdentifierScheme ?scheme .
        }}
        """
        batch_queries.append(query)
        batch_sizes.append(len(batch))

    all_bindings = _run_queries_parallel(endpoint_url, batch_queries, batch_sizes, workers, progress_callback)

    for bindings in all_bindings:
        for result in bindings:
            omid = result["omid"]["value"]
            val = result["val"]["value"]
            scheme_uri = result["scheme"]["value"]
            scheme = scheme_uri[len(DATACITE_PREFIX):] if scheme_uri.startswith(DATACITE_PREFIX) else scheme_uri
            id_key = f"{scheme}:{val}"
            if id_key not in found_omids:
                found_omids[id_key] = set()
            found_omids[id_key].add(omid)

    return found_omids

def find_file(rdf_dir: str, dir_split_number: int, items_per_file: int, uri: str, zip_output_rdf: bool) -> str|None:
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
    base_dir = os.path.dirname(data_zip_path)
    file_name = os.path.splitext(os.path.basename(data_zip_path))[0]
    prov_dir = os.path.join(base_dir, file_name, 'prov')
    prov_file = os.path.join(prov_dir, 'se.zip')
    return prov_file if os.path.exists(prov_file) else None

def process_csv_file(args: tuple, workers: int = QLEVER_MAX_WORKERS, progress=None, task_id=None) -> FileStats:
    csv_file, endpoint_url, prov_endpoint_url, rdf_dir, dir_split_number, items_per_file, zip_output_rdf = args

    def update_phase(phase: str):
        if progress and task_id is not None:
            progress.update(task_id, detail=phase)

    stats = FileStats(file=os.path.basename(csv_file))

    identifier_cache = {}

    unique_identifiers = set()
    row_identifiers = []

    update_phase("Phase 1/5: Reading CSV")
    rows = get_csv_data(csv_file)
    for row_num, row in enumerate(rows, 1):
        stats.total_rows += 1
        row_has_ids = False

        id_columns = ['id', 'author', 'editor', 'publisher', 'venue']

        for col in id_columns:
            identifiers = []

            if col == 'id':
                if row[col]:
                    identifiers = parse_identifiers(row[col])
            else:
                if row[col]:
                    elements = RE_SEMICOLON_IN_PEOPLE_FIELD.split(row[col])
                    for element in elements:
                        match = RE_NAME_AND_IDS.search(element)
                        if match and match.group(2):
                            ids_str = match.group(2)
                            identifiers.extend(parse_identifiers(ids_str))

            if identifiers:
                row_has_ids = True
                stats.total_identifiers += len(identifiers)
                for identifier in identifiers:
                    id_key = f"{identifier['schema']}:{identifier['value']}"
                    if identifier['schema'].lower() != 'omid':
                        unique_identifiers.add(id_key)
                    else:
                        stats.omid_schema_identifiers += 1
                    row_identifiers.append((row_num, col, identifier))

        if row_has_ids:
            stats.rows_with_ids += 1

    all_identifiers = []
    for id_key in unique_identifiers:
        parts = id_key.split(':', 1)
        if len(parts) == 2:
            all_identifiers.append({
                'schema': parts[0].lower(),
                'value': parts[1]
            })

    total_ids = len(all_identifiers)
    ids_processed = 0

    def on_id_batch(batch_size: int):
        nonlocal ids_processed
        ids_processed += batch_size
        update_phase(f"Phase 2/5: Querying DB [{ids_processed}/{total_ids} identifiers]")

    update_phase(f"Phase 2/5: Querying DB [0/{total_ids} identifiers]")
    identifier_cache = check_omids_existence(all_identifiers, endpoint_url, workers=workers, progress_callback=on_id_batch)

    update_phase("Phase 3/5: Mapping OMIDs to files")
    omids_by_file = {}
    all_omids = set()

    for row_num, col, identifier in row_identifiers:
        id_key = f"{identifier['schema']}:{identifier['value']}"

        omids: set = set()
        if identifier['schema'].lower() == 'omid':
            pass
        else:
            omids = identifier_cache.get(id_key, set())

            if omids:
                stats.identifiers_with_omids += 1
                all_omids.add(next(iter(omids)))

                for omid in omids:
                    zip_path = find_file(rdf_dir, dir_split_number, items_per_file, omid, zip_output_rdf)
                    if zip_path and os.path.exists(zip_path):
                        if zip_path not in omids_by_file:
                            omids_by_file[zip_path] = set()
                        omids_by_file[zip_path].add(omid)
            else:
                stats.identifiers_without_omids += 1

        stats.identifiers_details.append({
            'schema': identifier['schema'],
            'value': identifier['value'],
            'column': col,
            'has_omid': bool(omids),
            'row_number': row_num,
            'file': csv_file
        })

    total_rdf_files = len(omids_by_file)
    rdf_files_checked = 0
    update_phase(f"Phase 4/5: Checking RDF files [0/{total_rdf_files}]")
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
            data_found = data_content is not None and omid in data_content

            if prov_content is not None:
                prov_found = omid in prov_content
            else:
                prov_found = False

            if data_found:
                stats.data_graphs_found += 1
            else:
                stats.data_graphs_missing += 1

            if prov_found:
                stats.prov_graphs_found += 1
            else:
                stats.prov_graphs_missing += 1

            for row_num, col, identifier in row_identifiers:
                id_key = f"{identifier['schema']}:{identifier['value']}"
                if omid in identifier_cache.get(id_key, set()):
                    stats.processed_omids[omid] = {
                        'row': row_num,
                        'column': col,
                        'identifier': id_key,
                        'file': csv_file,
                        'data_found': data_found,
                        'prov_found': prov_found
                    }
                    break

        rdf_files_checked += 1
        update_phase(f"Phase 4/5: Checking RDF files [{rdf_files_checked}/{total_rdf_files}]")

    for row_num, col, identifier in row_identifiers:
        if identifier['schema'].lower() == 'omid':
            omid = identifier['value']
            if omid.startswith('http'):
                all_omids.add(omid)

    total_omids = len(all_omids)
    omids_checked = 0

    def on_prov_batch(batch_size: int):
        nonlocal omids_checked
        omids_checked += batch_size
        update_phase(f"Phase 5/5: Checking provenance [{omids_checked}/{total_omids} OMIDs]")

    update_phase(f"Phase 5/5: Checking provenance [0/{total_omids} OMIDs]")
    prov_results = check_provenance_existence(list(all_omids), prov_endpoint_url, workers=workers, progress_callback=on_prov_batch)

    for omid, has_prov in prov_results.items():
        if has_prov:
            stats.omids_with_provenance += 1
        else:
            stats.omids_without_provenance += 1

        if omid in stats.processed_omids:
            stats.processed_omids[omid]['triplestore_prov_found'] = has_prov

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Check MetaProcess results by verifying input CSV identifiers",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("meta_config", help="Path to meta_config.yaml file")
    parser.add_argument("output", help="Output file path for results (JSON)")
    parser.add_argument("--workers", type=int, default=QLEVER_MAX_WORKERS, help=f"Max parallel SPARQL workers (default: {QLEVER_MAX_WORKERS})")
    args = parser.parse_args()

    with open(args.meta_config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    input_csv_dir = config['input_csv_dir']
    base_output_dir = config['output_rdf_dir']
    output_rdf_dir = os.path.join(base_output_dir, 'rdf')
    endpoint_url = config['triplestore_url']
    prov_endpoint_url = config['provenance_triplestore_url']
    if not os.path.exists(output_rdf_dir):
        console.print(f"RDF directory not found at {output_rdf_dir}")
        return

    csv_files = sorted(collect_files(input_csv_dir, pattern="*.csv"))

    if not csv_files:
        console.print(f"No CSV files found in {input_csv_dir}")
        return

    console.print(f"Found {len(csv_files)} CSV files to process")

    output_dir = os.path.dirname(args.output) or '.'
    os.makedirs(output_dir, exist_ok=True)

    all_file_stats: list[FileStats] = []
    all_identifiers_details: list[dict] = []
    all_processed_omids: dict[str, dict] = {}
    id_key_to_omids: dict[str, set[str]] = {}

    process_args = [(f, endpoint_url, prov_endpoint_url, output_rdf_dir, config['dir_split_number'], config['items_per_file'], config['zip_output_rdf']) for f in csv_files]

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        EMATimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[current_file]}"),
        TextColumn("[yellow]{task.fields[detail]}"),
    ) as progress:
        task = progress.add_task("Processing CSV files", total=len(csv_files), current_file="", detail="")
        for idx, proc_args in enumerate(process_args):
            current_file = os.path.basename(csv_files[idx])
            progress.update(task, current_file=f"[{idx+1}/{len(csv_files)}] {current_file}")

            file_stats = process_csv_file(proc_args, workers=args.workers, progress=progress, task_id=task)
            all_file_stats.append(file_stats)

            all_identifiers_details.extend(file_stats.identifiers_details)
            all_processed_omids.update(file_stats.processed_omids)

            for omid, omid_details in file_stats.processed_omids.items():
                id_key = omid_details['identifier']
                if id_key not in id_key_to_omids:
                    id_key_to_omids[id_key] = set()
                id_key_to_omids[id_key].add(omid)

            progress.advance(task)

    # Build errors and warnings
    errors: list[dict] = []
    warnings: list[dict] = []

    for detail in all_identifiers_details:
        if not detail['has_omid'] and detail['schema'].lower() != 'omid':
            errors.append({
                "type": "missing_omid",
                "schema": detail['schema'],
                "value": detail['value'],
                "file": os.path.basename(detail['file']),
                "row": detail['row_number'],
                "column": detail['column'],
            })

    for omid, details in all_processed_omids.items():
        if details.get('triplestore_prov_found') is False:
            errors.append({
                "type": "missing_provenance",
                "omid": omid,
                "identifier": details['identifier'],
                "file": os.path.basename(details['file']),
                "row": details['row'],
            })

    # Cross-file: identifiers with multiple OMIDs
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        EMATimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[detail]}"),
    ) as progress:
        problematic: dict[str, dict] = {}
        task = progress.add_task("Checking cross-file issues", total=len(all_identifiers_details), detail="")
        for detail in all_identifiers_details:
            id_key = f"{detail['schema']}:{detail['value']}"
            omids = id_key_to_omids.get(id_key, set())

            if len(omids) > 1:
                if id_key not in problematic:
                    problematic[id_key] = {
                        'omids': omids,
                        'occurrences': []
                    }
                problematic[id_key]['occurrences'].append({
                    'file': os.path.basename(detail['file']),
                    'row': detail['row_number'],
                    'column': detail['column'],
                })
            progress.advance(task)
        progress.update(task, detail=f"Found {len(problematic)} identifiers with multiple OMIDs")

    for id_key, details in problematic.items():
        warnings.append({
            "type": "multiple_omids",
            "identifier": id_key,
            "omid_count": len(details['omids']),
            "omids": sorted(details['omids']),
            "occurrences": details['occurrences'],
        })

    # Aggregate summary
    summary = {
        "total_rows": sum(fs.total_rows for fs in all_file_stats),
        "rows_with_ids": sum(fs.rows_with_ids for fs in all_file_stats),
        "total_identifiers": sum(fs.total_identifiers for fs in all_file_stats),
        "omid_schema_identifiers": sum(fs.omid_schema_identifiers for fs in all_file_stats),
        "identifiers_with_omids": sum(fs.identifiers_with_omids for fs in all_file_stats),
        "identifiers_without_omids": sum(fs.identifiers_without_omids for fs in all_file_stats),
        "data_graphs_found": sum(fs.data_graphs_found for fs in all_file_stats),
        "data_graphs_missing": sum(fs.data_graphs_missing for fs in all_file_stats),
        "prov_graphs_found": sum(fs.prov_graphs_found for fs in all_file_stats),
        "prov_graphs_missing": sum(fs.prov_graphs_missing for fs in all_file_stats),
        "omids_with_provenance": sum(fs.omids_with_provenance for fs in all_file_stats),
        "omids_without_provenance": sum(fs.omids_without_provenance for fs in all_file_stats),
    }

    status = "PASS" if not errors else "FAIL"

    # Serialize per-file stats without internal details
    files_output = []
    for fs in all_file_stats:
        d = asdict(fs)
        del d['identifiers_details']
        del d['processed_omids']
        files_output.append(d)

    report = {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "config_path": os.path.abspath(args.meta_config),
        "total_files_processed": len(csv_files),
        "files": files_output,
        "summary": summary,
        "errors": errors,
        "warnings": warnings,
    }

    with open(args.output, 'wb') as f:
        f.write(orjson.dumps(report, option=orjson.OPT_INDENT_2))

    console.print(f"Status: {status}")
    console.print(f"Errors: {len(errors)}, Warnings: {len(warnings)}")
    console.print(f"Results written to: {args.output}")

    if errors:
        sys.exit(1)

if __name__ == "__main__":
    main()  # pragma: no cover
