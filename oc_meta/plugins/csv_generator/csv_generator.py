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


import json
import os
import re
from typing import Tuple
from zipfile import ZipFile

from oc_ocdm.reader import Reader
from oc_ocdm.storer import Storer
from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data, pathoo, write_csv

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

def generate_csv(rdf_dir: str, dir_split_number: str, items_per_file: str, output_dir: str, threshold: int) -> None:
    pathoo(output_dir)
    process_archives(rdf_dir, output_dir, process_br, threshold)
    print('[csv_generator: INFO] Solving the OpenCitations Meta Identifiers recursively')
    pbar = tqdm(total=len(os.listdir(output_dir)))
    for filename in os.listdir(output_dir):
        csv_data = get_csv_data(os.path.join(output_dir, filename))
        for row in csv_data:
            for identifier in [identifier for identifier in row['id'].split() if not identifier.startswith('meta')]:
                id_path = find_file(rdf_dir, dir_split_number, items_per_file, identifier)
                if id_path:
                    id_info = process_archive(id_path, process_id, identifier)
                    row['id'] = row['id'].replace(identifier, id_info)
            agents_by_role = {'author': dict(), 'editor': dict(), 'publisher': dict()}
            for agent in row['author'].split():
                agent_path = find_file(rdf_dir, dir_split_number, items_per_file, agent)
                if agent_path:
                    agent_info = process_archive(agent_path, process_agent, agent)
                    agents_by_role[agent_info['role']][agent] = agent_info
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
                        ra_path = find_file(rdf_dir, dir_split_number, items_per_file, ra)
                        if ra_path:
                            output_ra = process_archive(ra_path, process_responsible_agent, ra, rdf_dir, dir_split_number, items_per_file)
                            row[role] = row[role].replace(ra, output_ra)
            for venue in row['venue'].split():
                venue_path = find_file(rdf_dir, dir_split_number, items_per_file, venue)
                if venue_path:
                    to_be_found = venue
                    while to_be_found:
                        venue_info, to_be_found = process_archive(venue_path, process_venue, to_be_found, rdf_dir, dir_split_number, items_per_file)
                        for k, v in venue_info.items():
                            if v:
                                row[k] = v
                        if to_be_found:
                            venue_path = find_file(rdf_dir, dir_split_number, items_per_file, to_be_found)
            if row['page']:
                page_uri = row['page']
                page_path = find_file(rdf_dir, dir_split_number, items_per_file, page_uri)
                if page_path:
                    row['page'] = process_archive(page_path, process_page, page_uri)
        write_csv(os.path.join(output_dir, filename), csv_data, FIELDNAMES)
        pbar.update()
    pbar.close()

def process_archives(rdf_dir: str, output_dir: str, doing_what: callable, threshold: int):
    br_files = [os.path.join(fold, file) for fold, _, files in os.walk(os.path.join(rdf_dir, 'br')) for file in files if file.endswith('.zip') and os.path.basename(fold) != 'prov']
    print('[csv_generator: INFO] Looking for bibliographic resources recursively')
    pbar = tqdm(total=len(br_files))
    counter = 0
    global_output = list()
    for dirpath, _, filenames in os.walk(os.path.join(rdf_dir, 'br')):
        for filename in filenames:
            if filename.endswith('.zip') and os.path.basename(dirpath) != 'prov':
                local_output = process_archive(os.path.join(dirpath, filename), doing_what)
                global_output.extend(local_output)
                if len(global_output) > threshold:
                    write_csv(os.path.join(output_dir, f'{counter}.csv'), global_output, FIELDNAMES)
                    counter += 1
                    global_output = list()
                pbar.update()
    write_csv(os.path.join(output_dir, f'{counter}.csv'), global_output, FIELDNAMES)
    pbar.close()

def process_archive(filepath: str, doing_what: callable, *args) -> list:
    with ZipFile(file=filepath, mode="r") as archive:
        for zf_name in archive.namelist():
            with archive.open(zf_name) as f:
                br_data = json.load(f)
                return doing_what(br_data, *args)

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
            row['author'] = ' '.join(br_ars)
            row['page'] = br['http://purl.org/vocab/frbr/core#embodiment'][0]['@id'] if 'http://purl.org/vocab/frbr/core#embodiment' in br else ''
            if 'http://purl.org/vocab/frbr/core#partOf' in br:
                row['venue'] = br['http://purl.org/vocab/frbr/core#partOf'][0]['@id']
            csv_br_data.append(row)
    return csv_br_data

def process_id(id_data: list, id_uri: str) -> str:
    for graph in id_data:
        graph_data = graph['@graph']
        for id_data in graph_data:
            if id_data['@id'] == id_uri:
                id_schema = id_data['http://purl.org/spar/datacite/usesIdentifierScheme'][0]['@id'].split('/datacite/')[1]
                literal_value = id_data['http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'][0]['@value']
                return f'{id_schema}:{literal_value}'

def process_agent(agent_data: list, agent_uri: str) -> dict:
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
    return agent_dict

def process_responsible_agent(ra_data: list, ra_uri: str, rdf_dir: str, dir_split_number: str, items_per_file: str) -> str:
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
                        id_path = find_file(rdf_dir, dir_split_number, items_per_file, id_uri)
                        ra_ids.append(process_archive(id_path, process_id, id_uri))
                return f"{full_name} [{' '.join(ra_ids)}]"

def process_venue(venue_data: list, venue_uri: str, rdf_dir: str, dir_split_number: str, items_per_file: str) -> Tuple[dict, list]:
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
                    venue_title = venue['http://purl.org/dc/terms/title'][0]['@value']
                    venue_ids = venue['http://purl.org/spar/datacite/hasIdentifier']
                    explicit_ids = list()
                    for venue_id in venue_ids:
                        id_path = find_file(rdf_dir, dir_split_number, items_per_file, venue_id['@id'])
                        explicit_ids.append(process_archive(id_path, process_id, venue_id['@id']))
                    venue_dict['venue'] = f"{venue_title} [{' '.join(explicit_ids)}]"
                if 'http://purl.org/vocab/frbr/core#partOf' in venue:
                    to_be_found = venue['http://purl.org/vocab/frbr/core#partOf'][0]['@id']
    return venue_dict, to_be_found

def process_page(page_data: list, page_uri: str) -> str:
    for graph in page_data:
        graph_data = graph['@graph']
        for venue in graph_data:
            if venue['@id'] == page_uri:
                starting_page = venue['http://prismstandard.org/namespaces/basic/2.0/startingPage'][0]['@value']
                ending_page = venue['http://prismstandard.org/namespaces/basic/2.0/endingPage'][0]['@value']
                return f'{starting_page}-{ending_page}'

def find_file(rdf_dir: str, dir_split_number: str, items_per_file: str, uri: str) -> str|None:
    entity_regex: str = r'^(.+)/([a-z][a-z])/(0[1-9]+0)?([1-9][0-9]*)$'
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
        cur_file_path = os.path.join(cur_dir_path, str(cur_file_split)) + '.zip'
        return cur_file_path