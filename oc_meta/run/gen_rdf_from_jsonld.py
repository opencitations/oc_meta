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

import gzip
import argparse
import json
import multiprocessing
import os
import re
from typing import Match
from zipfile import ZIP_DEFLATED, ZipFile

from filelock import FileLock
from rdflib import ConjunctiveGraph, URIRef, Graph
import logging

# Variable used in several functions
entity_regex: str = r"^(.+)/([a-z][a-z])/(0[1-9]+0)?((?:[1-9][0-9]*)|(?:\d+-\d+))$"
prov_regex: str = r"^(.+)/([a-z][a-z])/(0[1-9]+0)?((?:[1-9][0-9]*)|(?:\d+-\d+))/prov/([a-z][a-z])/([1-9][0-9]*)$"

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

def _get_match(regex: str, group: int, string: str) -> str:
    match: Match = re.match(regex, string)
    if match is not None:
        return match.group(group)
    else:
        return ""

def get_base_iri(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return _get_match(prov_regex, 1, string_iri)
    else:
        return _get_match(entity_regex, 1, string_iri)

def get_short_name(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return _get_match(prov_regex, 5, string_iri)
    else:
        return _get_match(entity_regex, 2, string_iri)

def get_prov_subject_short_name(prov_res: URIRef) -> str:
    string_iri: str = str(prov_res)
    if "/prov/" in string_iri:
        return _get_match(prov_regex, 2, string_iri)
    else:
        return ""  # non-provenance entities do not have a prov_subject!

def get_prefix(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return ""  # provenance entities cannot have a supplier prefix
    else:
        return _get_match(entity_regex, 3, string_iri)

def get_prov_subject_prefix(prov_res: URIRef) -> str:
    string_iri: str = str(prov_res)
    if "/prov/" in string_iri:
        return _get_match(prov_regex, 3, string_iri)
    else:
        return ""  # non-provenance entities do not have a prov_subject!

def get_count(res: URIRef) -> str:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return _get_match(prov_regex, 6, string_iri)
    else:
        return _get_match(entity_regex, 4, string_iri)

def get_prov_subject_count(prov_res: URIRef) -> str:
    string_iri: str = str(prov_res)
    if "/prov/" in string_iri:
        return _get_match(prov_regex, 4, string_iri)
    else:
        return ""  # non-provenance entities do not have a prov_subject!

def get_resource_number(res: URIRef) -> int:
    string_iri: str = str(res)
    if "/prov/" in string_iri:
        return int(_get_match(prov_regex, 4, string_iri))
    else:
        return int(_get_match(entity_regex, 4, string_iri))

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

    cur_json_ld = json.loads(cur_g.serialize(format="json-ld"))
    file_lock = FileLock(f"{cur_file_path}.lock")

    with file_lock:
        if zip_output:
            with ZipFile(cur_file_path, mode="w", compression=ZIP_DEFLATED, allowZip64=True) as zip_file:
                json_str = json.dumps(cur_json_ld, ensure_ascii=False)
                zip_file.writestr(os.path.basename(cur_file_path.replace('.zip', '.json')), json_str)
        else:
            with open(cur_file_path, 'wt', encoding='utf-8') as f:
                json.dump(cur_json_ld, f, ensure_ascii=False)


def zip_files_in_directory(directory: str) -> None:
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_file_path = os.path.join(root, file)
                zip_file_path = json_file_path.replace('.json', '.zip')
                with ZipFile(zip_file_path, mode="w", compression=ZIP_LZMA, allowZip64=True) as zip_file:
                    zip_file.write(json_file_path, arcname=os.path.basename(json_file_path))
                os.remove(json_file_path)

def load_graph(file_path: str, cur_format: str = 'json-ld'):
    loaded_graph = ConjunctiveGraph()
    lock = FileLock(f"{file_path}.lock")
    with lock:
        if file_path.endswith('.zip'):
            with ZipFile(file=file_path, mode="r", compression=ZIP_DEFLATED, allowZip64=True) as archive:
                for zf_name in archive.namelist():
                    with archive.open(zf_name) as f:
                        if cur_format == "json-ld":
                            json_ld_file = json.load(f)
                            if isinstance(json_ld_file, dict):
                                json_ld_file = [json_ld_file]
                            for json_ld_resource in json_ld_file:
                                loaded_graph.parse(data=json.dumps(json_ld_resource, ensure_ascii=False), format=cur_format)
                        else:
                            loaded_graph.parse(file=f, format=cur_format)
        else:
            with open(file_path, 'rt', encoding='utf-8') as f:
                if cur_format == "json-ld":
                    json_ld_file = json.load(f)
                    if isinstance(json_ld_file, dict):
                        json_ld_file = [json_ld_file]
                    for json_ld_resource in json_ld_file:
                        loaded_graph.parse(data=json.dumps(json_ld_resource, ensure_ascii=False), format=cur_format)
                else:
                    loaded_graph.parse(file=f, format=cur_format)

    return loaded_graph

def process_graph(context, graph_identifier, output_root, base_iri, file_limit, item_limit, zip_output):
    modifications_by_file = {}
    triples = 0
    for triple in context:
        triples += len(triple)
        entity_uri = triple[0]
        _, cur_file_path = find_paths(entity_uri, output_root, base_iri, '_', file_limit, item_limit, True)
        cur_file_path = cur_file_path.replace('.json', '.zip') if zip_output else cur_file_path
        if cur_file_path not in modifications_by_file:
            modifications_by_file[cur_file_path] = {
                "graph_identifier": graph_identifier,
                "triples": []
            }
        modifications_by_file[cur_file_path]["triples"].append(triple)
    
    output_triples_count = 0
    for file_path, data in modifications_by_file.items():
        stored_g = load_graph(file_path) if os.path.exists(file_path) else ConjunctiveGraph()
        stored_g = store(data["triples"], data["graph_identifier"], stored_g)
        store_in_file(stored_g, file_path, zip_output)
    return triples

def process_file_content(graph: ConjunctiveGraph, output_root, base_iri, file_limit, item_limit, zip_output):
    for context in graph.contexts():
        graph_identifier = context.identifier
        process_graph(context, graph_identifier, output_root, base_iri, file_limit, item_limit, zip_output)

def main():
    parser = argparse.ArgumentParser(description="Process gzipped json-ld files into OC Meta RDF")
    parser.add_argument('input_folder', type=str, help='Input folder containing gzipped json-ld files')
    parser.add_argument('output_root', type=str, help='Root folder for output OC Meta RDF files')
    parser.add_argument('--base_iri', type=str, default='https://w3id.org/oc/meta/', help='The base URI of entities on Meta. This setting can be safely left as is')
    parser.add_argument('--file_limit', type=int, default=10000, help='Number of files per folder')
    parser.add_argument('--item_limit', type=int, default=1000, help='Number of items per file')
    parser.add_argument('-v', '--zip_output', dest='zip_output', action='store_true', required=False, help='Zip output json files')
    args = parser.parse_args()

    pool = multiprocessing.Pool()
    results = []
    for file in os.listdir(args.input_folder):
        if file.endswith('.jsonld.gz'):
            with gzip.open(os.path.join(args.input_folder, file), 'rb') as f:
                data = f.read().decode('utf-8')
                graph = ConjunctiveGraph()
                graph.parse(data=data, format='json-ld')
                result = pool.apply_async(process_file_content, (graph, args.output_root, args.base_iri, args.file_limit, args.item_limit, args.zip_output,))
                results.append(result)
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()