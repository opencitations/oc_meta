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
from typing import List, Tuple

import yaml
from rdflib import URIRef
from tqdm import tqdm

from oc_meta.plugins.csv_generator.csv_generator import (find_file,
                                                         process_archive)
from oc_meta.plugins.editor import MetaEditor


def find_broken_roles(filepath: str, meta_config: str, resp_agent: str, zip_output_rdf: bool):
    with open(meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    memory = dict()
    roles_in_br = process_archive(filepath, extract_roles_from_br)
    check_roles(roles_in_br, rdf_dir, dir_split_number, items_per_file, memory, meta_config, resp_agent, zip_output_rdf)

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

def check_roles(roles_in_br: List[list], rdf_dir: str, dir_split_number: str, items_per_file: str, memory: dict, meta_config: str, resp_agent: str, zip_output_rdf: bool) -> None:
    for roles_list in roles_in_br:
        last_roles = {'author': {'all': dict(), 'last': []}, 'editor': {'all': dict(), 'last': []}, 'publisher': {'all': dict(), 'last': []}}
        self_next = {'author': False, 'editor': False, 'publisher': False}
        for role in roles_list:
            ar_path = find_file(rdf_dir, dir_split_number, items_per_file, role, zip_output_rdf)
            role_next_tuple = process_archive(ar_path, get_next, memory, role)
            if role_next_tuple:
                agent_role = role_next_tuple[0]
                has_next = role_next_tuple[1]
                agent_role = agent_role.split('http://purl.org/spar/pro/')[1]
                if not has_next:
                    last_roles[agent_role]['last'].append(role)
                if has_next == role:
                    self_next[agent_role] = True
                last_roles[agent_role]['all'][role] = has_next
        fix_roles(last_roles, self_next, meta_config, resp_agent)

def fix_roles(last_roles: dict, self_next: dict, meta_config: str, resp_agent: str) -> None:
    meta_editor = MetaEditor(meta_config, resp_agent)
    for role_type, role_data in last_roles.items():
        all_list = list(role_data['all'].keys())
        last_list = role_data['last']
        if (all_list and len(last_list) != 1) or self_next[role_type]:
            sorted_roles_list = sorted(all_list)
            for i, role in enumerate(sorted_roles_list):
                if i < len(sorted_roles_list) - 1:
                    if last_roles[role_type]['all'][role] != sorted_roles_list[i+1]:
                        meta_editor.delete_property(URIRef(role), 'has_next')
                elif i == len(sorted_roles_list) - 1:
                    if last_roles[role_type]['all'][role]:
                        meta_editor.delete_property(URIRef(role), 'has_next')
            for i, role in enumerate(sorted_roles_list):
                if i < len(sorted_roles_list) - 1:
                    if last_roles[role_type]['all'][role] != sorted_roles_list[i+1]:
                        meta_editor.update_property(URIRef(role), 'has_next', URIRef(sorted_roles_list[i+1]))

def get_next(ar_data: list, ar_uri: str) -> Tuple[str, str]:
    for graph in ar_data:
        graph_data = graph['@graph']
        for agent in graph_data:
            if agent['@id'] == ar_uri:
                role = agent['http://purl.org/spar/pro/withRole'][0]['@id']
                if 'https://w3id.org/oc/ontology/hasNext' in agent:
                    return role, agent['https://w3id.org/oc/ontology/hasNext'][0]['@id']
                return role, ''