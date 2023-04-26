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

import json
import os
import re
from typing import Tuple
from zipfile import ZipFile

from filelock import FileLock
from rdflib import URIRef
from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data, write_csv
from oc_meta.plugins.editor import MetaEditor

URI_TYPE_DICT = {
    'http://purl.org/spar/fabio/ArchivalDocument': 'archival document', 
    'http://purl.org/spar/fabio/Book': 'book', 
    'http://purl.org/spar/fabio/BookChapter': 'book chapter', 
    'http://purl.org/spar/doco/Part': 'book part',
    'http://purl.org/spar/fabio/Expression': '',
    'http://purl.org/spar/fabio/ExpressionCollection': 'book section', 
    'http://purl.org/spar/fabio/BookSeries': 'book series', 
    'http://purl.org/spar/fabio/BookSet': 'book set', 
    'http://purl.org/spar/fabio/DataFile': 'dataset', 
    'http://purl.org/spar/fabio/Thesis': 'dissertation', 
    'http://purl.org/spar/fabio/Journal': 'journal', 
    'http://purl.org/spar/fabio/JournalArticle': 'journal article', 
    'http://purl.org/spar/fabio/JournalIssue': 'journal issue', 
    'http://purl.org/spar/fabio/JournalVolume': 'journal volume', 
    'http://purl.org/spar/fr/ReviewVersion': 'peer_review', 
    'http://purl.org/spar/fabio/AcademicProceedings': 'proceedings', 
    'http://purl.org/spar/fabio/ProceedingsPaper': 'proceedings article', 
    'http://purl.org/spar/fabio/ReferenceBook': 'reference book', 
    'http://purl.org/spar/fabio/ReferenceEntry': 'reference entry', 
    'http://purl.org/spar/fabio/ReportDocument': 'report', 
    'http://purl.org/spar/fabio/Series': 'series', 
    'http://purl.org/spar/fabio/SpecificationDocument': 'standard', 
    'http://purl.org/spar/fabio/WebContent': 'web content'}

FIELDNAMES = ['id', 'title', 'author', 'issue', 'volume', 'venue', 'page', 'pub_date', 'type', 'publisher', 'editor']

def generate_csv(filename, meta_config: str, rdf_dir, dir_split_number, items_per_file, resp_agent, output_dir: str, zip_output_rdf: bool) -> None:
    memory = dict()
    csv_data = get_csv_data(os.path.join(output_dir, filename))
    for row in csv_data:
        for identifier in [identifier for identifier in row['id'].split() if not identifier.startswith('meta')]:
            id_path = find_file(rdf_dir, dir_split_number, items_per_file, identifier, zip_output_rdf)
            if id_path:
                id_info = process_archive(id_path, process_id, memory, identifier, meta_config, resp_agent, id_path, memory)
                row['id'] = row['id'].replace(identifier, id_info)
        agents_by_role = {'author': dict(), 'editor': dict(), 'publisher': dict()}
        last_roles = {'author': {'all': dict(), 'last': []}, 'editor': {'all': dict(), 'last': []}, 'publisher': {'all': dict(), 'last': []}}
        self_next = {'author': False, 'editor': False, 'publisher': False}
        is_cache = True
        for agent in row['author'].split('; '):
            agent_path = find_file(rdf_dir, dir_split_number, items_per_file, agent, zip_output_rdf)
            if agent_path:
                agent_info = process_archive(agent_path, process_agent, memory, agent, meta_config, resp_agent, agent_path, memory)
                agent_role = agent_info['role']
                if not agent_info['next']:
                    last_roles[agent_role]['last'].append(agent)
                if agent_info['next'] == agent:
                    self_next[agent_role] = True
                last_roles[agent_role]['all'][agent] = agent_info['next']
                agents_by_role[agent_role][agent] = agent_info
                is_cache = False
        if not agents_by_role['author'] and not is_cache:
            row['author'] = ''
        if not is_cache:
            agents_by_role = fix_roles(last_roles, self_next, meta_config, resp_agent, agents_by_role)
        for agent_role, agents in agents_by_role.items():
            last = ''
            new_role_list = list()
            while agents:
                for agent, agent_data in agents.items():
                    if agent_data['next'] == last:
                        new_role_list.append(agent_data['ra'])
                        last = agent
                        del agents[agent]
                        break
            if new_role_list:
                row[agent_role] = '; '.join(reversed(new_role_list))
        for role in ['author', 'editor', 'publisher']:
            for ra in row[role].split('; '):
                if ra:
                    ra_path = find_file(rdf_dir, dir_split_number, items_per_file, ra, zip_output_rdf)
                    if ra_path:
                        output_ra = process_archive(ra_path, process_responsible_agent, memory, ra, rdf_dir, dir_split_number, items_per_file, memory, ra_path, meta_config, resp_agent, zip_output_rdf)
                        row[role] = row[role].replace(ra, output_ra)
        for venue in row['venue'].split():
            venue_path = find_file(rdf_dir, dir_split_number, items_per_file, venue, zip_output_rdf)
            if venue_path:
                to_be_found = venue
                while to_be_found:
                    venue_info, to_be_found = process_archive(venue_path, process_venue, memory, to_be_found, rdf_dir, dir_split_number, items_per_file, venue_path, meta_config, resp_agent, memory, zip_output_rdf)
                    for k, v in venue_info.items():
                        if v:
                            row[k] = v
                    if to_be_found:
                        venue_path = find_file(rdf_dir, dir_split_number, items_per_file, to_be_found, zip_output_rdf)
        if row['page']:
            page_uri = row['page']
            page_path = find_file(rdf_dir, dir_split_number, items_per_file, page_uri, zip_output_rdf)
            if page_path:
                row['page'] = process_archive(page_path, process_page, memory, page_uri, meta_config, resp_agent)
    write_csv(os.path.join(output_dir, filename), csv_data, FIELDNAMES)

def process_archives(rdf_dir: str, entity_abbr:str, output_dir: str|None, doing_what: callable, threshold: int=3000):
    br_files = [os.path.join(fold, file) for fold, _, files in os.walk(os.path.join(rdf_dir, entity_abbr)) for file in files if file.endswith('.zip') and os.path.basename(fold) != 'prov']
    print('[csv_generator: INFO] Looking for bibliographic resources recursively')
    pbar = tqdm(total=len(br_files))
    counter = 0
    global_output = list()
    for dirpath, _, filenames in os.walk(os.path.join(rdf_dir, entity_abbr)):
        for filename in filenames:
            if (filename.endswith('.zip') or filename.endswith('.json')) and os.path.basename(dirpath) != 'prov':
                local_output = process_archive(os.path.join(dirpath, filename), doing_what, None)
                global_output.extend(local_output)
                if len(global_output) > threshold and output_dir:
                    write_csv(os.path.join(output_dir, f'{counter}.csv'), global_output, FIELDNAMES)
                    counter += 1
                    global_output = list()
                pbar.update()
    if output_dir:
        write_csv(os.path.join(output_dir, f'{counter}.csv'), global_output, FIELDNAMES)
    pbar.close()

def process_archive(filepath: str, doing_what: callable, memory: dict|None = None, *args) -> list:
    filelock = FileLock(f'{filepath}.lock')
    if memory is not None:
        if filepath in memory:
            return doing_what(memory[filepath], *args)
    with filelock:
        try:
            if filepath.endswith('.zip'):
                with ZipFile(file=filepath, mode="r") as archive:
                    for zf_name in archive.namelist():
                        f = archive.open(zf_name)
            elif filepath.endswith('.json'):
                f = open(file=filepath, mode="r", encoding="utf8")
            data = json.load(f)
            f.close()
        except Exception as e:
            print('\nThe invalid zip file is', filepath, '\n', e)
            raise(Exception)
        if memory is not None:
            memory[filepath] = data
    return doing_what(data, *args)

def process_br(br_data: list) -> list:
    csv_br_data = list()
    for graph in br_data:
        graph_data = graph['@graph']
        for br in graph_data:
            row = dict()
            br_types = [x for x in br['@type'] if x != 'http://purl.org/spar/fabio/Expression']
            br_type = URI_TYPE_DICT[br_types[0]] if len(br_types) == 1 else ''
            if br_type in {'journal volume', 'journal issue'}:
                continue
            br_omid = f"meta:{br['@id'].split('/meta/')[1]}"
            br_identifiers = [br_omid]
            if 'http://purl.org/spar/datacite/hasIdentifier' in br:
                br_ids = br['http://purl.org/spar/datacite/hasIdentifier']
                for br_identifier in br_ids:
                    br_identifiers.append(br_identifier['@id'])
            row['id'] = ' '.join(br_identifiers)
            row['title'] = br['http://purl.org/dc/terms/title'][0]['@value'] if 'http://purl.org/dc/terms/title' in br else ''
            row['type'] = br_type
            row['pub_date'] = br['http://prismstandard.org/namespaces/basic/2.0/publicationDate'][0]['@value'] if 'http://prismstandard.org/namespaces/basic/2.0/publicationDate' in br else ''
            br_ars = list()
            if 'http://purl.org/spar/pro/isDocumentContextFor' in br:
                for ar_data in br['http://purl.org/spar/pro/isDocumentContextFor']:
                    br_ars.append(ar_data['@id'])
            row['author'] = '; '.join(br_ars)
            row['page'] = br['http://purl.org/vocab/frbr/core#embodiment'][0]['@id'] if 'http://purl.org/vocab/frbr/core#embodiment' in br else ''
            if 'http://purl.org/vocab/frbr/core#partOf' in br:
                row['venue'] = br['http://purl.org/vocab/frbr/core#partOf'][0]['@id']
            csv_br_data.append(row)
    return csv_br_data

def process_id(id_data: list, id_uri: str, meta_config: str, resp_agent: str, id_path: str, memory: dict) -> str:
    output_identifier = None
    for graph in id_data:
        graph_data = graph['@graph']
        for id_data in graph_data:
            if id_data['@id'] == id_uri:
                id_schema = id_data['http://purl.org/spar/datacite/usesIdentifierScheme'][0]['@id'].split('/datacite/')[1]
                literal_value = id_data['http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'][0]['@value']
                output_identifier = f'{id_schema}:{literal_value}'
    if output_identifier:
        return output_identifier
    else:
        del memory[id_path]
        meta_editor = MetaEditor(meta_config, resp_agent)
        meta_editor.sync_rdf_with_triplestore(id_uri)
        return process_archive(id_path, process_id, memory, id_uri, meta_config, resp_agent, id_path, memory)

def process_agent(agent_data: list, agent_uri: str, meta_config: str, resp_agent: str, agent_path: str, memory: dict) -> dict:
    agent_dict = dict()
    for graph in agent_data:
        graph_data = graph['@graph']
        for agent in graph_data:
            if agent['@id'] == agent_uri:
                agent_dict['ra'] = agent['http://purl.org/spar/pro/isHeldBy'][0]['@id']
                agent_dict['role'] = agent['http://purl.org/spar/pro/withRole'][0]['@id'].split('/pro/')[1]
                agent_dict['next'] = ''
                if 'https://w3id.org/oc/ontology/hasNext' in agent:
                    agent_dict['next'] = agent['https://w3id.org/oc/ontology/hasNext'][0]['@id']
    if agent_dict:
        return agent_dict
    else:
        del memory[agent_path]
        meta_editor = MetaEditor(meta_config, resp_agent)
        meta_editor.sync_rdf_with_triplestore(agent_uri)
        return process_archive(agent_path, process_agent, memory, agent_uri, meta_config, resp_agent, agent_path, memory)
                    
def process_responsible_agent(ra_data: list, ra_uri: str, rdf_dir: str, dir_split_number: str, items_per_file: str, memory: dict, ra_path: str, meta_config: str, resp_agent: str, zip_output_rdf: bool) -> str:
    ra_value = None
    for graph in ra_data:
        graph_data = graph['@graph']
        for ra in graph_data:
            if ra['@id'] == ra_uri:
                ra_name = ra['http://xmlns.com/foaf/0.1/name'][0]['@value'] if 'http://xmlns.com/foaf/0.1/name' in ra else None
                ra_fn = ra['http://xmlns.com/foaf/0.1/familyName'][0]['@value'] if 'http://xmlns.com/foaf/0.1/familyName' in ra else None
                ra_gn = ra['http://xmlns.com/foaf/0.1/givenName'][0]['@value'] if 'http://xmlns.com/foaf/0.1/givenName' in ra else None
                full_name = ''
                if ra_name:
                    full_name = ra_name
                else:
                    if ra_fn:
                        full_name += f'{ra_fn},'
                        if ra_gn:
                            full_name += f' {ra_gn}'
                    elif ra_gn:
                        full_name += f', {ra_gn}'
                ra_ids = [f"meta:{ra_uri.split('/meta/')[1]}"]
                if 'http://purl.org/spar/datacite/hasIdentifier' in ra:
                    for ra_identifier in ra['http://purl.org/spar/datacite/hasIdentifier']:
                        id_uri = ra_identifier['@id']
                        id_path = find_file(rdf_dir, dir_split_number, items_per_file, id_uri, zip_output_rdf)
                        ra_ids.append(process_archive(id_path, process_id, memory, id_uri, meta_config, resp_agent, id_path, memory))
                if full_name:
                    ra_value = f"{full_name} [{' '.join(ra_ids)}]"
                else:
                    ra_value = f"[{' '.join(ra_ids)}]"
    if ra_value:
        return ra_value
    else:
        del memory[ra_path]
        meta_editor = MetaEditor(meta_config, resp_agent)
        meta_editor.sync_rdf_with_triplestore(ra_uri)
        return process_archive(ra_path, process_responsible_agent, memory, ra_uri, rdf_dir, dir_split_number, items_per_file, memory, ra_path, meta_config, resp_agent, zip_output_rdf)

def process_venue(venue_data: list, venue_uri: str, rdf_dir: str, dir_split_number: str, items_per_file: str, venue_path: str, meta_config: str, resp_agent: str, memory: dict, zip_output_rdf: bool) -> Tuple[dict, list]:
    venue_dict = {'volume': '', 'issue': '', 'venue': ''}
    to_be_found = None
    for graph in venue_data:
        graph_data = graph['@graph']
        for venue in graph_data:
            if venue['@id'] == venue_uri:
                if 'http://purl.org/spar/fabio/JournalIssue' in venue['@type']:
                    venue_dict['issue'] = venue['http://purl.org/spar/fabio/hasSequenceIdentifier'][0]['@value']
                elif 'http://purl.org/spar/fabio/JournalVolume' in venue['@type']:
                    venue_dict['volume'] = venue['http://purl.org/spar/fabio/hasSequenceIdentifier'][0]['@value']
                else:
                    venue_title = venue['http://purl.org/dc/terms/title'][0]['@value'] if 'http://purl.org/dc/terms/title' in venue else None
                    venue_ids = venue['http://purl.org/spar/datacite/hasIdentifier'] if 'http://purl.org/spar/datacite/hasIdentifier' in venue else []
                    explicit_ids = [f"meta:{venue_uri.split('/meta/')[1]}"]
                    for venue_id in venue_ids:
                        id_path = find_file(rdf_dir, dir_split_number, items_per_file, venue_id['@id'], zip_output_rdf)
                        explicit_ids.append(process_archive(id_path, process_id, memory, venue_id['@id'], meta_config, resp_agent, id_path, memory))
                    venue_full = f"{venue_title} [{' '.join(explicit_ids)}]" if venue_title else f"[{' '.join(explicit_ids)}]"
                    venue_dict['venue'] = venue_full
                if 'http://purl.org/vocab/frbr/core#partOf' in venue:
                    to_be_found = venue['http://purl.org/vocab/frbr/core#partOf'][0]['@id']
    if any(v for _, v in venue_dict.items()):
        return venue_dict, to_be_found
    else:
        del memory[venue_path]
        meta_editor = MetaEditor(meta_config, resp_agent)
        meta_editor.sync_rdf_with_triplestore(venue_uri)
        return process_archive(venue_path, process_venue, memory, venue_uri, rdf_dir, dir_split_number, items_per_file, venue_path, meta_config, resp_agent, memory, zip_output_rdf)

def process_page(page_data: list, page_uri: str, meta_config: str, resp_agent: str) -> str:
    for graph in page_data:
        graph_data = graph['@graph']
        for venue in graph_data:
            if venue['@id'] == page_uri:
                starting_page = None
                ending_page = None
                if 'http://prismstandard.org/namespaces/basic/2.0/startingPage' in venue:
                    starting_page = venue['http://prismstandard.org/namespaces/basic/2.0/startingPage'][0]['@value']
                if 'http://prismstandard.org/namespaces/basic/2.0/endingPage' in venue:
                    ending_page = venue['http://prismstandard.org/namespaces/basic/2.0/endingPage'][0]['@value']
                if not all([starting_page, ending_page]):
                    meta_editor = MetaEditor(meta_config, resp_agent)
                    if not starting_page and ending_page:
                        meta_editor.update_property(URIRef(page_uri), 'has_starting_page', ending_page)
                        starting_page = ending_page
                    elif not ending_page and starting_page:
                        meta_editor.update_property(URIRef(page_uri), 'has_ending_page', starting_page)
                        ending_page = starting_page
                    else:
                        # TODO: delete entity
                        return ''
                return f'{starting_page}-{ending_page}'

def find_file(rdf_dir: str, dir_split_number: str, items_per_file: str, uri: str, zip_output_rdf: bool) -> str|None:
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

def fix_roles(last_roles: dict, self_next: dict, meta_config: str, resp_agent: str, agents_by_role: dict) -> dict:
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
            for agent_role, _ in agents_by_role.items():
                if agent_role == role_type:
                    new_agents = {sorted_role: {'next': sorted_roles_list[i+1], 'ra': agents_by_role[agent_role][sorted_role]['ra'], 'role': agents_by_role[agent_role][sorted_role]['role']} if i < len(sorted_roles_list) - 1 else {'next': '', 'ra': agents_by_role[agent_role][sorted_role]['ra'], 'role': agents_by_role[agent_role][sorted_role]['role']} for i, sorted_role in enumerate(sorted_roles_list)}
                    agents_by_role[agent_role] = new_agents
    return agents_by_role