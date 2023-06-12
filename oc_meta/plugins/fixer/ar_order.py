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


def find_broken_roles(filepath: str, meta_config: str, resp_agent: str, zip_output_rdf: bool, merge_ra: bool = False):
    with open(meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    memory = dict()
    roles_in_br = process_archive(filepath, extract_roles_from_br)
    check_roles(roles_in_br, rdf_dir, dir_split_number, items_per_file, memory, meta_config, resp_agent, zip_output_rdf, merge_ra)

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

def check_roles(roles_in_br: List[list], rdf_dir: str, dir_split_number: str, items_per_file: str, memory: dict, meta_config: str, resp_agent: str, zip_output_rdf: bool, merge_ra: bool = False) -> bool:
    order_changed_list = []
    meta_editor = MetaEditor(meta_config, resp_agent)
    for roles_list in roles_in_br:
        last_roles = {'author': {'has_next': dict(), 'ra': dict(), 'last': []}, 'editor': {'has_next': dict(), 'ra': dict(), 'last': []}, 'publisher': {'has_next': dict(), 'ra': dict(), 'last': []}}
        other_problems = {'author': {'self_next': False, 'multiple_has_next': False}, 'editor': {'self_next': False, 'multiple_has_next': False}, 'publisher': {'self_next': False, 'multiple_has_next': False}}
        to_be_merged = dict()
        for role in roles_list:
            ar_path = find_file(rdf_dir, dir_split_number, items_per_file, role, zip_output_rdf)
            role_data_tuple = process_archive(ar_path, get_ar_data, memory, role)
            if not role_data_tuple:
                meta_editor.sync_rdf_with_triplestore(role)
                role_data_tuple = process_archive(ar_path, get_ar_data, memory, role)
            if role_data_tuple:
                agent_role = role_data_tuple[0]
                has_next = role_data_tuple[1]
                resp_agent = role_data_tuple[2]
                agent_role = agent_role.split('http://purl.org/spar/pro/')[1]
                if resp_agent in last_roles[agent_role]['ra'] and merge_ra:
                    to_be_merged[last_roles[agent_role]['ra'][resp_agent]] = role
                    continue
                else:
                    last_roles[agent_role]['ra'][resp_agent] = role
                if len(has_next) == 0:
                    last_roles[agent_role]['last'].append(role)
                if any(next_role == role for next_role in has_next):
                    other_problems[agent_role]['self_next'] = True
                if len(has_next) > 1:
                    other_problems[agent_role]['multiple_has_next'] = True
                last_roles[agent_role]['has_next'][role] = has_next
        order_changed = fix_roles(last_roles, other_problems, to_be_merged, meta_config, resp_agent, merge_ra)
        order_changed_list.append(order_changed)
    return any(is_true for is_true in order_changed_list)

def fix_roles(last_roles: dict, other_problems: dict, to_be_merged: dict, meta_config: str, resp_agent: str, merge_ra: bool = False) -> bool:
    order_changed = False
    meta_editor = MetaEditor(meta_config, resp_agent)
    if merge_ra:
        for res, other in to_be_merged.items():
            meta_editor.merge(URIRef(res), URIRef(other))
    for role_type, role_data in last_roles.items():
        all_list = list(role_data['has_next'].keys())
        last_list = role_data['last']
        if (all_list and len(last_list) != 1) or other_problems[role_type]['self_next'] or other_problems[role_type]['multiple_has_next']:
            sorted_roles_list = sorted(all_list)
            for i, role in enumerate(sorted_roles_list):
                has_next_on_ts = last_roles[role_type]['has_next'][role][0] if last_roles[role_type]['has_next'][role] else ''
                if len(last_roles[role_type]['has_next'][role]) > 1:
                    meta_editor.delete(URIRef(role), 'has_next')
                    order_changed = True
                    last_roles[role_type]['has_next'][role] = []
                if i < len(sorted_roles_list) - 1:
                    if has_next_on_ts != sorted_roles_list[i+1]:
                        meta_editor.delete(URIRef(role), 'has_next')
                        order_changed = True
                elif i == len(sorted_roles_list) - 1:
                    if has_next_on_ts:
                        meta_editor.delete(URIRef(role), 'has_next')
                        order_changed = True
            for i, role in enumerate(sorted_roles_list):
                has_next_on_ts = last_roles[role_type]['has_next'][role][0] if last_roles[role_type]['has_next'][role] else ''
                if i < len(sorted_roles_list) - 1:
                    if has_next_on_ts != sorted_roles_list[i+1]:
                        meta_editor.update_property(URIRef(role), 'has_next', URIRef(sorted_roles_list[i+1]))
                        order_changed = True
    return order_changed

def get_ar_data(ar_data: list, ar_uri: str) -> Tuple[str, str, str]:
    for graph in ar_data:
        graph_data = graph['@graph']
        for agent in graph_data:
            if agent['@id'] == ar_uri:
                role = agent['http://purl.org/spar/pro/withRole'][0]['@id']
                ra = agent['http://purl.org/spar/pro/isHeldBy'][0]['@id']
                has_nexts = []
                if 'https://w3id.org/oc/ontology/hasNext' in agent:
                    for has_next in agent['https://w3id.org/oc/ontology/hasNext']:
                        has_nexts.append(has_next['@id'])
                    return role, has_nexts, ra
                return role, [], ra