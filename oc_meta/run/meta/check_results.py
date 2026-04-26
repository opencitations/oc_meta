# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import os
import sys
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Set

import orjson
import polars as pl
import yaml
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich_argparse import RichHelpFormatter
from oc_meta.constants import BR_ID_SCHEMAS, QLEVER_BATCH_SIZE, QLEVER_MAX_WORKERS, RA_ID_SCHEMAS
from oc_meta.lib.cleaner import normalize_hyphens, normalize_id
from oc_meta.lib.console import EMATimeRemainingColumn, console
from oc_meta.lib.file_manager import collect_files, find_rdf_file
from oc_meta.lib.master_of_regex import RE_SEMICOLON_IN_PEOPLE_FIELD, split_name_and_ids
from oc_meta.lib.sparql import run_queries_parallel

MAX_RETRIES = 10
RETRY_BACKOFF = 2
DATACITE_PREFIX = "http://purl.org/spar/datacite/"
RECOGNIZED_SCHEMAS = BR_ID_SCHEMAS | RA_ID_SCHEMAS

_SPACE_PATTERN = '[\t\xa0\u200b\u202f\u2003\u2005\u2009]'
_ID_COLUMNS = ['id', 'author', 'editor', 'publisher', 'venue']
_STAT_FIELDS = (
    'total_rows', 'rows_with_ids', 'total_identifiers', 'omid_schema_identifiers',
    'identifiers_skipped_invalid', 'identifiers_skipped_unverifiable',
    'identifiers_with_omids', 'identifiers_without_omids', 'identifiers_with_omid_mismatch',
    'data_graphs_found', 'data_graphs_missing', 'prov_graphs_found', 'prov_graphs_missing',
    'omids_with_provenance', 'omids_without_provenance',
)


@dataclass
class FileResult:
    file: str
    total_rows: int = 0
    rows_with_ids: int = 0
    total_identifiers: int = 0
    omid_schema_identifiers: int = 0
    identifiers_skipped_invalid: int = 0
    identifiers_skipped_unverifiable: int = 0
    identifiers_with_omids: int = 0
    identifiers_without_omids: int = 0
    identifiers_with_omid_mismatch: int = 0
    data_graphs_found: int = 0
    data_graphs_missing: int = 0
    prov_graphs_found: int = 0
    prov_graphs_missing: int = 0
    omids_with_provenance: int = 0
    omids_without_provenance: int = 0
    errors: list = field(default_factory=list)
    id_key_to_omids: dict = field(default_factory=dict)
    id_key_locations: dict = field(default_factory=dict)


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

    all_bindings = run_queries_parallel(prov_endpoint_url, batch_queries, batch_sizes, workers, progress_callback, max_retries=MAX_RETRIES, backoff_factor=RETRY_BACKOFF)

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

        SELECT ?val ?scheme ?entity
        WHERE {{
            VALUES (?val ?scheme) {{ {" ".join(values_entries)} }}
            ?id_entity literal:hasLiteralValue ?val ;
                       datacite:usesIdentifierScheme ?scheme .
            ?entity datacite:hasIdentifier ?id_entity .
        }}
        """
        batch_queries.append(query)
        batch_sizes.append(len(batch))

    all_bindings = run_queries_parallel(endpoint_url, batch_queries, batch_sizes, workers, progress_callback, max_retries=MAX_RETRIES, backoff_factor=RETRY_BACKOFF)

    for bindings in all_bindings:
        for result in bindings:
            entity = result["entity"]["value"]
            val = result["val"]["value"]
            scheme_uri = result["scheme"]["value"]
            scheme = scheme_uri[len(DATACITE_PREFIX):] if scheme_uri.startswith(DATACITE_PREFIX) else scheme_uri
            id_key = f"{scheme}:{val}"
            if id_key not in found_omids:
                found_omids[id_key] = set()
            found_omids[id_key].add(entity)

    return found_omids




def find_prov_file(data_zip_path: str) -> str|None:
    base_dir = os.path.dirname(data_zip_path)
    file_name = os.path.splitext(os.path.basename(data_zip_path))[0]
    prov_dir = os.path.join(base_dir, file_name, 'prov')
    prov_file = os.path.join(prov_dir, 'se.zip')
    return prov_file if os.path.exists(prov_file) else None


def _check_zip_file(args: tuple) -> tuple[str, dict[str, tuple[bool, bool]]]:
    zip_path, omids = args

    raw_data = b''
    with zipfile.ZipFile(zip_path, 'r') as z:
        json_files = [f for f in z.namelist() if f.endswith('.json')]
        if json_files:
            with z.open(json_files[0]) as f:
                raw_data = f.read()

    raw_prov = b''
    prov_path = find_prov_file(zip_path)
    if prov_path:
        with zipfile.ZipFile(prov_path, 'r') as z:
            json_files = [f for f in z.namelist() if f.endswith('.json')]
            if json_files:
                with z.open(json_files[0]) as f:
                    raw_prov = f.read()

    results: dict[str, tuple[bool, bool]] = {}
    for omid in omids:
        omid_bytes = (omid + '"').encode()
        results[omid] = (omid_bytes in raw_data, (omid + '/prov/"').encode() in raw_prov)
    return zip_path, results


def _extract_entity_groups(cell: str, col: str, base_iri: str) -> list[dict]:
    base = base_iri.rstrip('/')

    def parse_tokens(token_str: str) -> dict:
        group: dict = {'omid_uri': None, 'recognized': [], 'unverifiable': []}
        for token in token_str.strip().split():
            colon_pos = token.find(':')
            if colon_pos <= 0:
                continue
            schema = token[:colon_pos].lower()
            value = normalize_hyphens(token[colon_pos + 1:])
            if schema == 'omid':
                group['omid_uri'] = f"{base}/{value}"
            elif schema in RECOGNIZED_SCHEMAS:
                group['recognized'].append((schema, value))
            else:
                group['unverifiable'].append((schema, value))
        return group

    if col == 'id':
        return [parse_tokens(cell)]

    groups = []
    for element in RE_SEMICOLON_IN_PEOPLE_FIELD.split(cell):
        element = element.strip()
        if not element:
            continue
        _, ids_str = split_name_and_ids(element)
        if not ids_str:
            continue
        groups.append(parse_tokens(ids_str))
    return groups


def process_csv_file(args: tuple, workers: int = QLEVER_MAX_WORKERS, progress=None, task_id=None) -> FileResult:
    csv_file, endpoint_url, prov_endpoint_url, rdf_dir, dir_split_number, items_per_file, zip_output_rdf, base_iri = args

    result = FileResult(file=os.path.basename(csv_file))

    if progress and task_id is not None:
        progress.update(task_id, detail="Phase 1/5: Reading CSV")

    with open(csv_file, 'rb') as f:
        raw = f.read()
    df = pl.read_csv(raw.replace(b'\0', b''), columns=_ID_COLUMNS, infer_schema_length=0)
    df = df.with_columns([
        pl.col(c)
        .str.replace_all(_SPACE_PATTERN, ' ')
        .str.replace_all('&nbsp;', ' ', literal=True)
        for c in _ID_COLUMNS
    ])

    result.total_rows = len(df)
    col_lists = {col: df[col].to_list() for col in _ID_COLUMNS}
    del df

    # entity URIs from CSV OMID tokens — used for data graph + prov verification
    all_entity_uris: set[str] = set()
    entity_uri_to_info: dict[str, tuple[int, str]] = {}  # uri → (row_num, col)

    # recognized IDs for SPARQL lookup
    recognized_ids: list[dict] = []
    recognized_id_set: set[str] = set()
    recognized_id_to_csv_omid: dict[str, str | None] = {}
    recognized_id_occurrences: dict[str, list[tuple[int, str]]] = {}
    recognized_id_meta: dict[str, tuple[str, str]] = {}

    phase1_task = None
    if progress:
        phase1_task = progress.add_task("  Phase 1/5: Extracting identifiers", total=result.total_rows, detail="")

    for row_idx in range(result.total_rows):
        row_has_ids = False
        row_num = row_idx + 1

        for col in _ID_COLUMNS:
            cell = col_lists[col][row_idx]
            if not cell:
                continue

            groups = _extract_entity_groups(cell, col, base_iri)
            for group in groups:
                n_tokens = (1 if group['omid_uri'] else 0) + len(group['recognized']) + len(group['unverifiable'])
                if n_tokens == 0:
                    continue

                row_has_ids = True
                result.total_identifiers += n_tokens

                if group['omid_uri']:
                    result.omid_schema_identifiers += 1
                    all_entity_uris.add(group['omid_uri'])
                    if group['omid_uri'] not in entity_uri_to_info:
                        entity_uri_to_info[group['omid_uri']] = (row_num, col)

                result.identifiers_skipped_unverifiable += len(group['unverifiable'])

                for schema, value in group['recognized']:
                    normalized = normalize_id(f"{schema}:{value}")
                    if not normalized:
                        result.identifiers_skipped_invalid += 1
                        continue
                    norm_schema, norm_value = normalized.split(':', 1)
                    id_key = normalized
                    if id_key not in recognized_id_set:
                        recognized_id_set.add(id_key)
                        recognized_ids.append({'schema': norm_schema, 'value': norm_value})
                        recognized_id_to_csv_omid[id_key] = group['omid_uri']
                        recognized_id_meta[id_key] = (norm_schema, norm_value)
                        recognized_id_occurrences[id_key] = []
                    recognized_id_occurrences[id_key].append((row_num, col))

        if row_has_ids:
            result.rows_with_ids += 1

        if progress and phase1_task is not None:
            progress.advance(phase1_task)

    if progress and phase1_task is not None:
        progress.update(phase1_task, visible=False)

    del col_lists

    phase2_task = None
    if progress:
        phase2_task = progress.add_task("  Phase 2/5: Querying DB", total=len(recognized_ids), detail="")

    def on_id_batch(batch_size: int):
        if progress and phase2_task is not None:
            progress.advance(phase2_task, batch_size)

    identifier_cache = check_omids_existence(recognized_ids, endpoint_url, workers=workers, progress_callback=on_id_batch)

    if progress and phase2_task is not None:
        progress.update(phase2_task, visible=False)

    omids_by_file: dict[str, set[str]] = {}
    path_exists_cache: dict[str, bool] = {}
    omid_to_id_info: dict[str, tuple[int, str, str]] = {}
    csv_basename = os.path.basename(csv_file)

    phase3_task = None
    if progress:
        phase3_task = progress.add_task("  Phase 3/5: Mapping OMIDs", total=len(recognized_id_set), detail="")

    for id_key in recognized_id_set:
        occurrences = recognized_id_occurrences[id_key]
        expected_omid = recognized_id_to_csv_omid[id_key]
        schema, value = recognized_id_meta[id_key]

        if id_key in identifier_cache:
            entity_uris = identifier_cache[id_key]
            result.id_key_to_omids[id_key] = entity_uris
            result.id_key_locations[id_key] = [
                {'file': csv_basename, 'row': r, 'column': c}
                for r, c in occurrences
            ]

            if expected_omid and expected_omid not in entity_uris:
                result.identifiers_with_omid_mismatch += len(occurrences)
                for row_num, col in occurrences:
                    result.errors.append({
                        "type": "omid_mismatch",
                        "schema": schema,
                        "value": value,
                        "expected_omid": expected_omid,
                        "found_omids": sorted(entity_uris),
                        "file": csv_basename,
                        "row": row_num,
                        "column": col,
                    })
            else:
                result.identifiers_with_omids += len(occurrences)
        else:
            result.identifiers_without_omids += len(occurrences)
            for row_num, col in occurrences:
                result.errors.append({
                    "type": "identifier_not_in_triplestore",
                    "schema": schema,
                    "value": value,
                    "file": csv_basename,
                    "row": row_num,
                    "column": col,
                })

        if progress and phase3_task is not None:
            progress.advance(phase3_task)

    if progress and phase3_task is not None:
        progress.update(phase3_task, visible=False)

    # Build file map from all entity URIs collected from CSV OMIDs
    for entity_uri in all_entity_uris:
        if entity_uri not in omid_to_id_info:
            if entity_uri in entity_uri_to_info:
                row_num, col = entity_uri_to_info[entity_uri]
                omid_to_id_info[entity_uri] = (row_num, col, entity_uri)
        zip_path = find_rdf_file(entity_uri, rdf_dir, dir_split_number, items_per_file, zip_output_rdf)
        if zip_path not in path_exists_cache:
            path_exists_cache[zip_path] = os.path.exists(zip_path)
        if path_exists_cache[zip_path]:
            if zip_path in omids_by_file:
                omids_by_file[zip_path].add(entity_uri)
            else:
                omids_by_file[zip_path] = {entity_uri}

    total_rdf_files = len(omids_by_file)
    total_omids = len(all_entity_uris)

    prov_future = None
    prov_executor = None
    if total_omids > 0:
        prov_executor = ProcessPoolExecutor(
            max_workers=1,
            mp_context=multiprocessing.get_context('forkserver')
        )
        prov_future = prov_executor.submit(
            check_provenance_existence, list(all_entity_uris), prov_endpoint_url, workers
        )

    phase4_task = None
    if progress:
        phase4_task = progress.add_task("  Phase 4/5: Checking RDF files", total=total_rdf_files, detail="")

    zip_args = [(zp, list(omids)) for zp, omids in omids_by_file.items()]

    def _apply_zip_results(zip_results: dict[str, tuple[bool, bool]]) -> None:
        for _, (data_found, prov_found) in zip_results.items():
            if data_found:
                result.data_graphs_found += 1
            else:
                result.data_graphs_missing += 1
            if prov_found:
                result.prov_graphs_found += 1
            else:
                result.prov_graphs_missing += 1

    if zip_args and workers > 1:
        with ProcessPoolExecutor(
            max_workers=min(len(zip_args), workers),
            mp_context=multiprocessing.get_context('forkserver')
        ) as executor:
            for future in as_completed(
                {executor.submit(_check_zip_file, a): a for a in zip_args}
            ):
                _zip_path, zip_results = future.result()
                _apply_zip_results(zip_results)
                if progress and phase4_task is not None:
                    progress.advance(phase4_task)
    else:
        for a in zip_args:
            _zip_path, zip_results = _check_zip_file(a)
            _apply_zip_results(zip_results)
            if progress and phase4_task is not None:
                progress.advance(phase4_task)

    if progress and phase4_task is not None:
        progress.update(phase4_task, visible=False)

    phase5_task = None
    if progress:
        phase5_task = progress.add_task("  Phase 5/5: Checking provenance", total=total_omids, detail="")

    prov_results: Dict[str, bool] = {}
    if prov_future and prov_executor:
        prov_results = prov_future.result()
        prov_executor.shutdown(wait=False)

    if progress and phase5_task is not None:
        progress.advance(phase5_task, total_omids)
        progress.update(phase5_task, visible=False)

    for omid, has_prov in prov_results.items():
        if has_prov:
            result.omids_with_provenance += 1
        else:
            result.omids_without_provenance += 1
            if omid in omid_to_id_info:
                row_num, col, id_key = omid_to_id_info[omid]
                result.errors.append({
                    "type": "missing_provenance",
                    "omid": omid,
                    "identifier": id_key,
                    "file": csv_basename,
                    "row": row_num,
                })

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Check MetaProcess results by verifying output CSV identifiers against the triplestore",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("meta_config", help="Path to meta_config.yaml file")
    parser.add_argument("output", help="Output file path for results (JSON)")
    parser.add_argument(
        "--csv",
        help="Path to a single output CSV file or directory of output CSVs. "
             "Defaults to {base_output_dir}/csv/ from meta_config.",
        default=None,
    )
    parser.add_argument("--workers", type=int, default=QLEVER_MAX_WORKERS, help=f"Max parallel SPARQL workers (default: {QLEVER_MAX_WORKERS})")
    args = parser.parse_args()

    with open(args.meta_config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    if args.csv:
        csv_source = args.csv
        if os.path.isfile(csv_source):
            csv_files = [csv_source]
        else:
            csv_files = collect_files(csv_source, pattern="*.csv")
    else:
        output_csv_dir = os.path.join(config['base_output_dir'], 'csv')
        csv_files = collect_files(output_csv_dir, pattern="*.csv")

    base_output_dir = config['base_output_dir']
    output_rdf_dir = os.path.join(base_output_dir, 'rdf')
    endpoint_url = config['triplestore_url']
    prov_endpoint_url = config['provenance_triplestore_url']
    base_iri = config['base_iri'].rstrip('/') + '/'

    if not os.path.exists(output_rdf_dir):
        console.print(f"RDF directory not found at {output_rdf_dir}")
        return

    if not csv_files:
        console.print("No CSV files found")
        return

    console.print(f"Found {len(csv_files)} CSV files to process")

    output_dir = os.path.dirname(args.output) or '.'
    os.makedirs(output_dir, exist_ok=True)

    all_file_results: list[FileResult] = []

    process_args = [
        (f, endpoint_url, prov_endpoint_url, output_rdf_dir,
         config['dir_split_number'], config['items_per_file'], config['zip_output_rdf'], base_iri)
        for f in csv_files
    ]

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[cyan]{task.completed:.0f}/{task.total:.0f}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        EMATimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[detail]}"),
    ) as progress:
        task = progress.add_task("Processing CSV files", total=len(csv_files), detail="")

        if len(csv_files) > 1:
            max_procs = min(len(csv_files), 4)
            file_workers = max(1, args.workers // max_procs)
            with ProcessPoolExecutor(
                max_workers=max_procs,
                mp_context=multiprocessing.get_context('forkserver')
            ) as executor:
                future_to_idx = {
                    executor.submit(process_csv_file, pa, workers=file_workers): idx
                    for idx, pa in enumerate(process_args)
                }
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    current_file = os.path.basename(csv_files[idx])
                    progress.update(task, detail=f"Completed {current_file}")
                    all_file_results.append(future.result())
                    progress.advance(task)
        else:
            for idx, proc_args in enumerate(process_args):
                current_file = os.path.basename(csv_files[idx])
                progress.update(task, detail=f"[{idx+1}/{len(csv_files)}] {current_file}")
                file_result = process_csv_file(proc_args, workers=args.workers, progress=progress, task_id=task)
                all_file_results.append(file_result)
                progress.advance(task)

    errors: list[dict] = []
    merged_id_key_to_omids: dict[str, set[str]] = {}
    merged_id_key_locations: dict[str, list[dict]] = {}

    for fr in all_file_results:
        errors.extend(fr.errors)
        for id_key, omids in fr.id_key_to_omids.items():
            if id_key in merged_id_key_to_omids:
                merged_id_key_to_omids[id_key].update(omids)
            else:
                merged_id_key_to_omids[id_key] = set(omids)
        for id_key, locs in fr.id_key_locations.items():
            if id_key in merged_id_key_locations:
                merged_id_key_locations[id_key].extend(locs)
            else:
                merged_id_key_locations[id_key] = list(locs)

    warnings: list[dict] = []
    for id_key, omids in merged_id_key_to_omids.items():
        if len(omids) > 1:
            warnings.append({
                "type": "multiple_omids",
                "identifier": id_key,
                "omid_count": len(omids),
                "omids": sorted(omids),
                "occurrences": merged_id_key_locations[id_key],
            })

    summary = {k: 0 for k in _STAT_FIELDS}
    files_output = []
    for fr in all_file_results:
        file_dict = {k: getattr(fr, k) for k in ('file',) + _STAT_FIELDS}
        files_output.append(file_dict)
        for k in _STAT_FIELDS:
            summary[k] += getattr(fr, k)

    status = "PASS" if not errors else "FAIL"

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
