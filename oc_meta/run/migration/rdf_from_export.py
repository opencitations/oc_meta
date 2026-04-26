#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import gzip
import logging
import multiprocessing
import os
import re
import time
import uuid
from zipfile import ZIP_DEFLATED, ZipFile

import orjson
from rdflib import Dataset
from rdflib.exceptions import ParserError
from oc_ocdm.support.support import find_paths
from rich_argparse import RichHelpFormatter
from tqdm import tqdm

def store(triples, graph_identifier, stored_g: Dataset) -> Dataset:
    for triple in triples:
        stored_g.add((triple[0], triple[1], triple[2], graph_identifier))
    return stored_g

def store_in_file(cur_g: Dataset, cur_file_path: str, zip_output: bool) -> None:
    dir_path = os.path.dirname(cur_file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    cur_json_ld = orjson.loads(cur_g.serialize(format="json-ld"))

    if zip_output:
        with ZipFile(cur_file_path, mode="w", compression=ZIP_DEFLATED, allowZip64=True) as zip_file:
            json_str = orjson.dumps(cur_json_ld).decode('utf-8')
            zip_file.writestr(os.path.basename(cur_file_path.replace('.zip', '.json')), json_str)
    else:
        with open(cur_file_path, 'wb') as f:
            f.write(orjson.dumps(cur_json_ld))

def load_graph(file_path: str, cur_format: str = 'json-ld'):
    loaded_graph = Dataset(default_union=True)
    if file_path.endswith('.zip'):
        with ZipFile(file=file_path, mode="r", compression=ZIP_DEFLATED, allowZip64=True) as archive:
            for zf_name in archive.namelist():
                with archive.open(zf_name) as f:
                    if cur_format == "json-ld":
                        json_ld_file = orjson.loads(f.read())
                        if isinstance(json_ld_file, dict):
                            json_ld_file = [json_ld_file]
                        for json_ld_resource in json_ld_file:
                            loaded_graph.parse(data=orjson.dumps(json_ld_resource).decode('utf-8'), format=cur_format)
                    else:
                        loaded_graph.parse(file=f, format=cur_format)  # type: ignore[arg-type]
    else:
        with open(file_path, 'rb') as f:
            if cur_format == "json-ld":
                json_ld_file = orjson.loads(f.read())
                if isinstance(json_ld_file, dict):
                    json_ld_file = [json_ld_file]
                for json_ld_resource in json_ld_file:
                    loaded_graph.parse(data=orjson.dumps(json_ld_resource).decode('utf-8'), format=cur_format)
            else:
                loaded_graph.parse(file=f, format=cur_format)  # type: ignore[arg-type]

    return loaded_graph

def process_graph(context, graph_identifier, output_root, base_iri, file_limit, item_limit, zip_output):
    modifications_by_file = {}
    triples = 0
    unique_id = generate_unique_id()

    for triple in context:
        triples += len(triple)
        entity_uri = triple[0]
        _, cur_file_path = find_paths(entity_uri, output_root, base_iri, '_', file_limit, item_limit, True)
        if cur_file_path is None:
            logging.warning(f"Skipping triple due to invalid URI: {entity_uri}")
            continue
        
        # Estrai il nome base del file (numero) e aggiungi l'ID unico
        base_name = os.path.splitext(os.path.basename(cur_file_path))[0]
        new_file_name = f"{base_name}_{unique_id}"
        
        cur_file_path = os.path.join(os.path.dirname(cur_file_path), new_file_name + ('.zip' if zip_output else '.json'))

        if cur_file_path not in modifications_by_file:
            modifications_by_file[cur_file_path] = {
                "graph_identifier": graph_identifier,
                "triples": []
            }
        modifications_by_file[cur_file_path]["triples"].append(triple)

    for file_path, data in modifications_by_file.items():
        stored_g = load_graph(file_path) if os.path.exists(file_path) else Dataset()
        stored_g = store(data["triples"], data["graph_identifier"], stored_g)
        store_in_file(stored_g, file_path, zip_output)
    return triples

def merge_files(output_root, base_file_name, file_extension, zip_output):
    """Funzione per fondere i file generati dai diversi processi"""
    files_to_merge = [f for f in os.listdir(output_root) if f.startswith(base_file_name) and f.endswith(file_extension)]
    
    merged_graph = Dataset()

    for file_path in files_to_merge:
        cur_full_path = os.path.join(output_root, file_path)
        loaded_graph = load_graph(cur_full_path)
        merged_graph += loaded_graph

    final_file_path = os.path.join(output_root, base_file_name + file_extension)
    store_in_file(merged_graph, final_file_path, zip_output)  # type: ignore[arg-type]

def merge_files_in_directory(directory, zip_output, stop_file):
    """Function to merge files in a specific directory"""
    if check_stop_file(stop_file):
        logging.info("Stop file detected. Stopping merge process.")
        return

    files = [f for f in os.listdir(directory) if f.endswith('.zip' if zip_output else '.json')]
    
    # Group files by their base name (number without the unique ID)
    file_groups = {}
    for file in files:
        match = re.match(r'^((?:\d+)|(?:se))(?:_[^.]+)?\.', file)
        if match:
            base_name = match.group(1)
            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(file)
    
    for base_file_name, files_to_merge in file_groups.items():
        if check_stop_file(stop_file):
            logging.info("Stop file detected. Stopping merge process.")
            return

        # Only proceed with merging if there's at least one file with an underscore
        if not any('_' in file for file in files_to_merge):
            continue

        merged_graph = Dataset()

        for file_path in files_to_merge:
            cur_full_path = os.path.join(directory, file_path)
            loaded_graph = load_graph(cur_full_path)
            for context in loaded_graph.graphs():
                graph_identifier = context.identifier
                for triple in context:
                    merged_graph.add(triple + (graph_identifier,))  # type: ignore[arg-type]
        
        final_file_path = os.path.join(directory, f"{base_file_name}" + ('.zip' if zip_output else '.json'))
        store_in_file(merged_graph, final_file_path, zip_output)

        # Remove the original files after merging
        for file_path in files_to_merge:
            if file_path != os.path.basename(final_file_path):
                os.remove(os.path.join(directory, file_path))

def generate_unique_id():
    return f"{int(time.time())}-{uuid.uuid4()}"

def merge_files_wrapper(args):
    directory, zip_output, stop_file = args
    merge_files_in_directory(directory, zip_output, stop_file)

def merge_all_files_parallel(output_root, zip_output, stop_file):
    """Function to merge files in parallel"""
    if check_stop_file(stop_file):
        logging.info("Stop file detected. Stopping merge process.")
        return

    directories_to_process = []
    for root, dirs, files in os.walk(output_root):
        if any(f.endswith('.zip' if zip_output else '.json') for f in files):
            directories_to_process.append(root)

    with multiprocessing.Pool() as pool:
        list(tqdm(pool.imap(merge_files_wrapper, 
                            [(dir, zip_output, stop_file) for dir in directories_to_process]), 
                  total=len(directories_to_process), 
                  desc="Merging files in directories"))

def process_file_content(file_path, output_root, base_iri, file_limit, item_limit, zip_output, rdf_format):
    with gzip.open(file_path, 'rb') as f:
        content = f.read()
        assert isinstance(content, bytes)
        data = content.decode('utf-8')
        graph = Dataset()
        try:
            graph.parse(data=data, format=rdf_format)
        except ParserError as e:
            logging.error(f"Failed to parse {file_path}: {e}")
            return

    for context in graph.graphs():
        graph_identifier = context.identifier
        process_graph(context, graph_identifier, output_root, base_iri, file_limit, item_limit, zip_output)

def process_file_wrapper(args):
    file_path, output_root, base_iri, file_limit, item_limit, zip_output, rdf_format, cache_file, stop_file = args
    if check_stop_file(stop_file):
        return
    if not is_file_processed(file_path, cache_file):
        process_file_content(file_path, output_root, base_iri, file_limit, item_limit, zip_output, rdf_format)
        mark_file_as_processed(file_path, cache_file)

def process_chunk(chunk, output_root, base_iri, file_limit, item_limit, zip_output, rdf_format, cache_file, stop_file):
    with multiprocessing.Pool() as pool:
        list(tqdm(pool.imap(process_file_wrapper, 
                            [(file_path, output_root, base_iri, file_limit, item_limit, zip_output, rdf_format, cache_file, stop_file) 
                             for file_path in chunk]), 
                  total=len(chunk), 
                  desc="Processing files"))

def create_cache_file(cache_file):
    if cache_file:
        if not os.path.exists(cache_file):
            with open(cache_file, 'w', encoding='utf8'):
                pass  # Create an empty file
    else:
        logging.info("No cache file specified. Skipping cache creation.")

def is_file_processed(file_path, cache_file):
    if not cache_file:
        return False
    with open(cache_file, 'r', encoding='utf8') as f:
        processed_files = f.read().splitlines()
    return file_path in processed_files

def mark_file_as_processed(file_path, cache_file):
    if cache_file:
        with open(cache_file, 'a', encoding='utf8') as f:
            f.write(f"{file_path}\n")
    else:
        logging.debug(f"No cache file specified. Skipping marking {file_path} as processed.")

def check_stop_file(stop_file):
    return os.path.exists(stop_file)

def main():
    parser = argparse.ArgumentParser(
        description="Process gzipped input files into OC Meta RDF",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument('input_folder', type=str, help='Input folder containing gzipped input files')
    parser.add_argument('output_root', type=str, help='Root folder for output OC Meta RDF files')
    parser.add_argument('--base_iri', type=str, default='https://w3id.org/oc/meta/', help='The base URI of entities on Meta')
    parser.add_argument('--file_limit', type=int, default=10000, help='Number of files per folder')
    parser.add_argument('--item_limit', type=int, default=1000, help='Number of items per file')
    parser.add_argument('-v', '--zip_output', default=True, dest='zip_output', action='store_true', help='Zip output json files')
    parser.add_argument('--input_format', type=str, default='jsonld', choices=['jsonld', 'nquads'], help='Format of the input files')
    parser.add_argument('--chunk_size', type=int, default=1000, help='Number of files to process before merging')
    parser.add_argument('--cache_file', type=str, default=None, help='File to store processed file names (optional)')
    parser.add_argument('--stop_file', type=str, default='./.stop', help='File to signal process termination')
    args = parser.parse_args()

    create_cache_file(args.cache_file)

    file_extension = '.nq.gz' if args.input_format == 'nquads' else '.jsonld.gz'
    rdf_format = 'nquads' if args.input_format == 'nquads' else 'json-ld'

    files_to_process = [os.path.join(args.input_folder, file) for file in os.listdir(args.input_folder) if file.endswith(file_extension)]
    chunks = [files_to_process[i:i + args.chunk_size] for i in range(0, len(files_to_process), args.chunk_size)]
    for i, chunk in enumerate(tqdm(chunks, desc="Processing chunks")):
        if check_stop_file(args.stop_file):
            logging.info("Stop file detected. Gracefully terminating the process.")
            break
        logging.info(f"Processing chunk {i+1}/{len(chunks)}")
        process_chunk(chunk, args.output_root, args.base_iri, args.file_limit, args.item_limit, args.zip_output, rdf_format, args.cache_file, args.stop_file)
        logging.info(f"Merging files for chunk {i+1}")
        merge_all_files_parallel(args.output_root, args.zip_output, args.stop_file)

    logging.info("Processing complete")

if __name__ == "__main__":
    main()
