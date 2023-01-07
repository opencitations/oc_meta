#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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


from __future__ import annotations

import os
from typing import List

import yaml
from rdflib import URIRef
from tqdm import tqdm

from oc_meta.plugins.csv_generator.csv_generator import (find_file,
                                                         process_archive)
from oc_meta.plugins.editor import MetaEditor


def find_broken_roles(meta_config: str, resp_agent: str):
    meta_editor = MetaEditor(meta_config, resp_agent)
    with open(meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    memory = dict()
    br_files = [os.path.join(fold, file) for fold, _, files in os.walk(os.path.join(rdf_dir, 'br')) for file in files if file.endswith('.zip') and os.path.basename(fold) != 'prov']
    print('[csv_generator: INFO] Looking for bibliographic resources recursively')
    pbar = tqdm(total=len(br_files))
    for dirpath, _, filenames in os.walk(os.path.join(rdf_dir, 'br')):
        for filename in filenames:
            if filename.endswith('.zip') and os.path.basename(dirpath) != 'prov':
                roles_in_br = process_archive(os.path.join(dirpath, filename), extract_roles_from_br)
                fix_roles(roles_in_br, rdf_dir, dir_split_number, items_per_file, memory, meta_editor)
                pbar.update()
    pbar.close()

def extract_roles_from_br(br_data: list) -> list:
    all_ar = list()
    for graph in br_data:
        graph_data = graph['@graph']
        for br in graph_data:
            br_ars = list()
            if 'http://purl.org/spar/pro/isDocumentContextFor' in br:
                for ar_data in br['http://purl.org/spar/pro/isDocumentContextFor']:
                    br_ars.append(ar_data['@id'])
            if br_ars:
                all_ar.append(br_ars)
    return all_ar

def fix_roles(roles_in_br: List[list], rdf_dir: str, dir_split_number: str, items_per_file: str, memory: dict, meta_editor: MetaEditor):
    for roles_list in roles_in_br:
        last_roles = list()
        self_next = False
        for role in roles_list:
            ar_path = find_file(rdf_dir, dir_split_number, items_per_file, role)
            has_next = process_archive(ar_path, get_next, memory, role)
            if not has_next:
                last_roles.append(role)
            if has_next == role:
                self_next = True
        if len(last_roles) != 1 or self_next:
            sorted_roles_list = sorted(roles_list)
            for role in sorted_roles_list:
                meta_editor.delete_property(URIRef(role), 'has_next')
            for i, role in enumerate(sorted_roles_list):
                if i > 0:
                    meta_editor.update_property(URIRef(sorted_roles_list[i-1]), 'has_next', URIRef(role))
            meta_editor.save()

def get_next(ar_data: list, ar_uri: str) -> str:
    for graph in ar_data:
        graph_data = graph['@graph']
        for agent in graph_data:
            if agent['@id'] == ar_uri:
                if 'https://w3id.org/oc/ontology/hasNext' in agent:
                    return agent['https://w3id.org/oc/ontology/hasNext'][0]['@id']
                return ''