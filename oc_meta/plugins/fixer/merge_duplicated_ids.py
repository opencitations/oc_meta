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
# SOFTWARE.

import os

import yaml
from rdflib import URIRef

from oc_meta.plugins.csv_generator.csv_generator import (find_file,
                                                         process_archive,
                                                         process_id)
from oc_meta.plugins.editor import MetaEditor


def find_duplicated_ids_in_entity_type(filepath: str, meta_config: str, entity_type_abbr: str, resp_agent: str, zip_output_rdf: bool):
    with open(meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    rdf_entity_dir = os.path.join(rdf_dir, entity_type_abbr) + os.sep 
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    memory = dict()
    for dirpath, _, filenames in os.walk(rdf_entity_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            process_archive(filepath, extract_identifiers, memory, rdf_dir, dir_split_number, items_per_file, zip_output_rdf, memory, meta_config, resp_agent)

def extract_identifiers(data: list, rdf_dir:str, dir_split_number: int, items_per_file: int, zip_output_rdf: bool, memory: dict, meta_config: str, resp_agent: str):
    for graph in data:
        graph_data = graph['@graph']
        for entity in graph_data:
            entity_ids = list()
            ids_mapping = dict()
            if 'http://purl.org/spar/datacite/hasIdentifier' in entity:
                identifiers = entity['http://purl.org/spar/datacite/hasIdentifier']
                for identifier in identifiers:
                    id_uri = identifier['@id']
                    id_path = find_file(rdf_dir, dir_split_number, items_per_file, id_uri, zip_output_rdf)
                    id_full = process_archive(id_path, process_id, memory, id_uri, meta_config, resp_agent, id_path, memory)
                    ids_mapping[id_uri] = id_full
                    entity_ids.append(id_full)
            if len(entity_ids) > len(set(entity_ids)):
                merge_repeated_ids(ids_mapping, meta_config, resp_agent)

def merge_repeated_ids(ids_mapping: dict, meta_config: str, resp_agent: str) -> list:
    meta_editor = MetaEditor(meta_config, resp_agent)
    new_list = []
    for id_uri, id_full in ids_mapping.items():
        if id_full in new_list:
            prev_id_uri = next(prev_uri for prev_uri, prev_full in ids_mapping.items() if prev_full == id_full)
            meta_editor.merge(URIRef(prev_id_uri), URIRef(id_uri))
        else:
            new_list.append(id_full)
    return new_list