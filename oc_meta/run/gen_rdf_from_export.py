#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2023 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE

import argparse
import gzip
import logging
import multiprocessing
import os
import re
import time
import uuid
from functools import lru_cache
from typing import Match
from zipfile import ZIP_DEFLATED, ZipFile

import orjson
import rdflib
from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm

# Variable used in several functions
entity_regex: str = r"^(.+)/([a-z][a-z])/(0[1-9]+0)?((?:[1-9][0-9]*)|(?:\d+-\d+))$"
prov_regex: str = r"^(.+)/([a-z][a-z])/(0[1-9]+0)?((?:[1-9][0-9]*)|(?:\d+-\d+))/prov/([a-z][a-z])/([1-9][0-9]*)$"

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

@lru_cache(maxsize=1024)
def _get_match_cached(regex: str, group: int, string: str) -> str:
    match: Match = re.match(regex, string)
    if match is not None:
        return match.group(group)
    else:
        return ""

def get_base_iri(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return _get_match_cached(prov_regex, 1, string_iri)
    else:
        return _get_match_cached(entity_regex, 1, string_iri)

def get_short_name(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return _get_match_cached(prov_regex, 5, string_iri)
    else:
        return _get_match_cached(entity_regex, 2, string_iri)

def get_prov_subject_short_name(prov_res: URIRef) -> str:
    string_iri: str = str(prov_res)
    if "/prov/" in string_iri:
        return _get_match_cached(prov_regex, 2, string_iri)
    else:
        return ""  # non-provenance entities do not have a prov_subject!

def get_prefix(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return ""  # provenance entities cannot have a supplier prefix
    else:
        return _get_match_cached(entity_regex, 3, string_iri)

def get_prov_subject_prefix(prov_res: URIRef) -> str:
    string_iri: str = str(prov_res)
    if "/prov/" in string_iri:
        return _get_match_cached(prov_regex, 3, string_iri)
    else:
        return ""  # non-provenance entities do not have a prov_subject!

def get_count(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return _get_match_cached(prov_regex, 6, string_iri)
    else:
        return _get_match_cached(entity_regex, 4, string_iri)

def get_prov_subject_count(prov_res: URIRef) -> str:
    string_iri: str = str(prov_res)
    if "/prov/" in string_iri:
        return _get_match_cached(prov_regex, 4, string_iri)
    else:
        return ""  # non-provenance entities do not have a prov_subject!

def get_resource_number(res: URIRef) -> int:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        match = _get_match_cached(prov_regex, 4, string_iri)
    else:
        match = _get_match_cached(entity_regex, 4, string_iri)
    if not match:
        logging.warning(f"Could not extract resource number from URI: {string_iri}")
        return -1  # or some other default value
    try:
        return int(match)
    except ValueError:
        logging.error(f"Invalid resource number in URI: {string_iri}, extracted: {match}")
        return -1  # or some other default value

def find_local_line_id(res: URIRef, n_file_item: int = 1) -> int:
    cur_number: int = get_resource_number(res)

    cur_file_split: int = 0
    while True:
        if cur_number > cur_file_split:
            cur_file_split += n_file_item
        else:
            cur_file_split -= n_file_item
            break

    return cur_number - cur_file_split

def find_paths(res: URIRef, base_dir: str, base_iri: str, default_dir: str, dir_split: int,
               n_file_item: int, is_json: bool = True):
    """
    This function is responsible for looking for the correct JSON file that contains the data related to the
    resource identified by the variable 'string_iri'. This search takes into account the organisation in
    directories and files, as well as the particular supplier prefix for bibliographic entities, if specified.
    In case no supplier prefix is specified, the 'default_dir' (usually set to "_") is used instead.
    """
    string_iri: str = str(res)

    cur_number: int = get_resource_number(res)

    if cur_number == -1:
        # Handle the error case
        logging.error(f"Could not process URI: {string_iri}")
        return None, None  # or some default paths

    # Find the correct file number where to save the resources
    cur_file_split: int = 0
    while True:
        if cur_number > cur_file_split:
            cur_file_split += n_file_item
        else:
            break

    # The data have been split in multiple directories and it is not something related
    # with the provenance data of the whole corpus (e.g. provenance agents)
    if dir_split and not string_iri.startswith(base_iri + "prov/"):
        # Find the correct directory number where to save the file
        cur_split: int = 0
        while True:
            if cur_number > cur_split:
                cur_split += dir_split
            else:
                break

        if "/prov/" in string_iri:  # provenance file of a bibliographic entity
            subj_short_name: str = get_prov_subject_short_name(res)
            short_name: str = get_short_name(res)
            sub_folder: str = get_prov_subject_prefix(res)
            file_extension: str = '.json' if is_json else '.nq'
            if sub_folder == "":
                sub_folder = default_dir
            if sub_folder == "":
                sub_folder = "_"  # enforce default value

            cur_dir_path: str = base_dir + subj_short_name + os.sep + sub_folder + \
                os.sep + str(cur_split) + os.sep + str(cur_file_split) + os.sep + "prov"
            cur_file_path: str = cur_dir_path + os.sep + short_name + file_extension
        else:  # regular bibliographic entity
            short_name: str = get_short_name(res)
            sub_folder: str = get_prefix(res)
            file_extension: str = '.json' if is_json else '.nt'
            if sub_folder == "":
                sub_folder = default_dir
            if sub_folder == "":
                sub_folder = "_"  # enforce default value

            cur_dir_path: str = base_dir + short_name + os.sep + sub_folder + os.sep + str(cur_split)
            cur_file_path: str = cur_dir_path + os.sep + str(cur_file_split) + file_extension
    # Enter here if no split is needed
    elif dir_split == 0:
        if "/prov/" in string_iri:
            subj_short_name: str = get_prov_subject_short_name(res)
            short_name: str = get_short_name(res)
            sub_folder: str = get_prov_subject_prefix(res)
            file_extension: str = '.json' if is_json else '.nq'
            if sub_folder == "":
                sub_folder = default_dir
            if sub_folder == "":
                sub_folder = "_"  # enforce default value

            cur_dir_path: str = base_dir + subj_short_name + os.sep + sub_folder + \
                os.sep + str(cur_file_split) + os.sep + "prov"
            cur_file_path: str = cur_dir_path + os.sep + short_name + file_extension
        else:
            short_name: str = get_short_name(res)
            sub_folder: str = get_prefix(res)
            file_extension: str = '.json' if is_json else '.nt'
            if sub_folder == "":
                sub_folder = default_dir
            if sub_folder == "":
                sub_folder = "_"  # enforce default value

            cur_dir_path: str = base_dir + short_name + os.sep + sub_folder
            cur_file_path: str = cur_dir_path + os.sep + str(cur_file_split) + file_extension
    # Enter here if the data is about a provenance agent, e.g. /corpus/prov/
    else:
        short_name: str = get_short_name(res)
        prefix: str = get_prefix(res)
        count: str = get_count(res)
        file_extension: str = '.json' if is_json else '.nq'

        cur_dir_path: str = base_dir + short_name
        cur_file_path: str = cur_dir_path + os.sep + prefix + count + file_extension

    return cur_dir_path, cur_file_path

def store(triples, graph_identifier, stored_g: ConjunctiveGraph) -> ConjunctiveGraph:
    for triple in triples:
        stored_g.add((triple[0], triple[1], triple[2], graph_identifier))
    return stored_g

def store_in_file(cur_g: ConjunctiveGraph, cur_file_path: str, zip_output: bool) -> None:
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
    loaded_graph = ConjunctiveGraph()
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
                        loaded_graph.parse(file=f, format=cur_format)
    else:
        with open(file_path, 'rb', encoding='utf-8') as f:
            if cur_format == "json-ld":
                json_ld_file = orjson.loads(f.read())
                if isinstance(json_ld_file, dict):
                    json_ld_file = [json_ld_file]
                for json_ld_resource in json_ld_file:
                    loaded_graph.parse(data=orjson.dumps(json_ld_resource).decode('utf-8'), format=cur_format)
            else:
                loaded_graph.parse(file=f, format=cur_format)

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
        stored_g = load_graph(file_path) if os.path.exists(file_path) else ConjunctiveGraph()
        stored_g = store(data["triples"], data["graph_identifier"], stored_g)
        store_in_file(stored_g, file_path, zip_output)
    return triples

def merge_files(output_root, base_file_name, file_extension, zip_output):
    """Funzione per fondere i file generati dai diversi processi"""
    files_to_merge = [f for f in os.listdir(output_root) if f.startswith(base_file_name) and f.endswith(file_extension)]
    
    merged_graph = ConjunctiveGraph()

    for file_path in files_to_merge:
        cur_full_path = os.path.join(output_root, file_path)
        loaded_graph = load_graph(cur_full_path)
        merged_graph += loaded_graph

    final_file_path = os.path.join(output_root, base_file_name + file_extension)
    store_in_file(merged_graph, final_file_path, zip_output)

def merge_files_in_directory(directory, zip_output, stop_file):
    """Function to merge files in a specific directory"""
    if check_stop_file(stop_file):
        print("Stop file detected. Stopping merge process.")
        return

    files = [f for f in os.listdir(directory) if f.endswith('.zip' if zip_output else '.json')]
    
    # Group files by their base name (number without the unique ID)
    file_groups = {}
    for file in files:
        match = re.match(r'^(\d+)(?:_[^.]+)?\.', file)
        if match:
            base_name = match.group(1)
            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(file)
    
    for base_file_name, files_to_merge in file_groups.items():
        if check_stop_file(stop_file):
            print("Stop file detected. Stopping merge process.")
            return

        # Only proceed with merging if there's at least one file with an underscore
        if not any('_' in file for file in files_to_merge):
            continue

        merged_graph = ConjunctiveGraph()

        for file_path in files_to_merge:
            cur_full_path = os.path.join(directory, file_path)
            loaded_graph = load_graph(cur_full_path)
            for context in loaded_graph.contexts():
                graph_identifier = context.identifier
                for triple in context:
                    merged_graph.add(triple + (graph_identifier,))
        
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
        print("Stop file detected. Stopping merge process.")
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
        data = f.read().decode('utf-8')
        graph = ConjunctiveGraph()
        try:
            graph.parse(data=data, format=rdf_format)
        except rdflib.exceptions.ParserError as e:
            logging.error(f"Failed to parse {file_path}: {e}")
            return

    for context in graph.contexts():
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
            with open(cache_file, 'w', encoding='utf8') as f:
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
    parser = argparse.ArgumentParser(description="Process gzipped input files into OC Meta RDF")
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
            print("Stop file detected. Gracefully terminating the process.")
            break
        print(f"\nProcessing chunk {i+1}/{len(chunks)}")
        process_chunk(chunk, args.output_root, args.base_iri, args.file_limit, args.item_limit, args.zip_output, rdf_format, args.cache_file, args.stop_file)
        print(f"Merging files for chunk {i+1}")
        merge_all_files_parallel(args.output_root, args.zip_output, args.stop_file)

    print("Processing complete")

if __name__ == "__main__":
    main()
