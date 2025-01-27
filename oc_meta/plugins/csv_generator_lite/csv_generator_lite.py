#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2024 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import csv
import json
import os
import re
from typing import Dict, List, Optional
from zipfile import ZipFile
from multiprocessing import Pool, Lock, Value
import math

from tqdm import tqdm

# Constants
FIELDNAMES = ['id', 'title', 'author', 'issue', 'volume', 'venue', 'page', 'pub_date', 'type', 'publisher', 'editor']

URI_TYPE_DICT = {
    'http://purl.org/spar/doco/Abstract': 'abstract',
    'http://purl.org/spar/fabio/ArchivalDocument': 'archival document',
    'http://purl.org/spar/fabio/AudioDocument': 'audio document',
    'http://purl.org/spar/fabio/Book': 'book',
    'http://purl.org/spar/fabio/BookChapter': 'book chapter',
    'http://purl.org/spar/fabio/ExpressionCollection': 'book section', 
    'http://purl.org/spar/fabio/BookSeries': 'book series', 
    'http://purl.org/spar/fabio/BookSet': 'book set',
    'http://purl.org/spar/fabio/ComputerProgram': 'computer program',
    'http://purl.org/spar/doco/Part': 'book part',
    'http://purl.org/spar/fabio/Expression': '',
    'http://purl.org/spar/fabio/DataFile': 'dataset',
    'http://purl.org/spar/fabio/DataManagementPlan': 'data management plan',
    'http://purl.org/spar/fabio/Thesis': 'dissertation',
    'http://purl.org/spar/fabio/Editorial': 'editorial',
    'http://purl.org/spar/fabio/Journal': 'journal', 
    'http://purl.org/spar/fabio/JournalArticle': 'journal article',
    'http://purl.org/spar/fabio/JournalEditorial': 'journal editorial',
    'http://purl.org/spar/fabio/JournalIssue': 'journal issue', 
    'http://purl.org/spar/fabio/JournalVolume': 'journal volume',
    'http://purl.org/spar/fabio/Newspaper': 'newspaper',
    'http://purl.org/spar/fabio/NewspaperArticle': 'newspaper article',
    'http://purl.org/spar/fabio/NewspaperIssue': 'newspaper issue',
    'http://purl.org/spar/fr/ReviewVersion': 'peer review', 
    'http://purl.org/spar/fabio/AcademicProceedings': 'proceedings',
    'http://purl.org/spar/fabio/Preprint': 'preprint',
    'http://purl.org/spar/fabio/Presentation': 'presentation',
    'http://purl.org/spar/fabio/ProceedingsPaper': 'proceedings article', 
    'http://purl.org/spar/fabio/ReferenceBook': 'reference book', 
    'http://purl.org/spar/fabio/ReferenceEntry': 'reference entry', 
    'http://purl.org/spar/fabio/ReportDocument': 'report', 
    'http://purl.org/spar/fabio/RetractionNotice': 'retraction notice',
    'http://purl.org/spar/fabio/Series': 'series', 
    'http://purl.org/spar/fabio/SpecificationDocument': 'standard', 
    'http://purl.org/spar/fabio/WebContent': 'web content'}

def find_file(rdf_dir: str, dir_split_number: int, items_per_file: int, uri: str) -> Optional[str]:
    """Find the file path for a given URI based on the directory structure"""
    entity_regex: str = r'^(https:\/\/w3id\.org\/oc\/meta)\/([a-z][a-z])\/(0[1-9]+0)?([1-9][0-9]*)$'
    entity_match = re.match(entity_regex, uri)
    if entity_match:
        cur_number = int(entity_match.group(4))
        cur_file_split = ((cur_number - 1) // items_per_file) * items_per_file + items_per_file
        cur_split = ((cur_number - 1) // dir_split_number) * dir_split_number + dir_split_number
        
        short_name = entity_match.group(2)
        sub_folder = entity_match.group(3) or ''
        cur_dir_path = os.path.join(rdf_dir, short_name, sub_folder, str(cur_split))
        cur_file_path = os.path.join(cur_dir_path, str(cur_file_split)) + '.zip'
        
        return cur_file_path if os.path.exists(cur_file_path) else None
    return None

def load_json_from_file(filepath: str) -> dict:
    """Load JSON data from a ZIP file"""
    try:
        with ZipFile(filepath, 'r') as zip_file:
            json_filename = zip_file.namelist()[0]
            with zip_file.open(json_filename) as json_file:
                # Decode the bytes using UTF-8 encoding before parsing JSON
                json_content = json_file.read().decode('utf-8')
                return json.loads(json_content)
    except Exception as e:
        print(f"Error loading file {filepath}: {e}")
        return {}

def process_identifier(id_data: dict) -> Optional[str]:
    """Process identifier data to extract schema and value"""
    try:
        id_schema = id_data['http://purl.org/spar/datacite/usesIdentifierScheme'][0]['@id'].split('/datacite/')[1]
        literal_value = id_data['http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'][0]['@value']
        return f'{id_schema}:{literal_value}'
    except (KeyError, IndexError):
        return None

def process_responsible_agent(ra_data: dict, ra_uri: str, rdf_dir: str, dir_split_number: int, items_per_file: int) -> Optional[str]:
    """Process responsible agent data to extract name and identifiers"""
    try:
        # Process name
        family_name = ra_data.get('http://xmlns.com/foaf/0.1/familyName', [{}])[0].get('@value', '')
        given_name = ra_data.get('http://xmlns.com/foaf/0.1/givenName', [{}])[0].get('@value', '')
        foaf_name = ra_data.get('http://xmlns.com/foaf/0.1/name', [{}])[0].get('@value', '')
        
        # Determine name format
        if family_name or given_name:
            if family_name and given_name:
                name = f"{family_name}, {given_name}"
            elif family_name:
                name = f"{family_name},"
            else:
                name = f", {given_name}"
        elif foaf_name:
            name = foaf_name
        else:
            return None

        # Get OMID
        omid = ra_uri.split('/')[-1]
        identifiers = [f'omid:ra/{omid}']
        
        # Process other identifiers
        if 'http://purl.org/spar/datacite/hasIdentifier' in ra_data:
            for identifier in ra_data['http://purl.org/spar/datacite/hasIdentifier']:
                id_uri = identifier['@id']
                id_file = find_file(rdf_dir, dir_split_number, items_per_file, id_uri)
                if id_file:
                    id_data = load_json_from_file(id_file)
                    for graph in id_data:
                        for entity in graph.get('@graph', []):
                            if entity['@id'] == id_uri:
                                id_value = process_identifier(entity)
                                if id_value:
                                    identifiers.append(id_value)
        
        if identifiers:
            return f"{name} [{' '.join(identifiers)}]"
        return name
    except (KeyError, IndexError):
        return None

def process_venue_title(venue_data: dict, venue_uri: str, rdf_dir: str, dir_split_number: int, items_per_file: int) -> str:
    """Process venue title and its identifiers"""
    venue_title = venue_data.get('http://purl.org/dc/terms/title', [{}])[0].get('@value', '')
    if not venue_title:
        return ''
        
    # Get OMID
    omid = venue_uri.split('/')[-1]
    identifiers = [f'omid:br/{omid}']
    
    # Process other identifiers
    if 'http://purl.org/spar/datacite/hasIdentifier' in venue_data:
        for identifier in venue_data['http://purl.org/spar/datacite/hasIdentifier']:
            id_uri = identifier['@id']
            id_file = find_file(rdf_dir, dir_split_number, items_per_file, id_uri)
            if id_file:
                id_data = load_json_from_file(id_file)
                for graph in id_data:
                    for entity in graph.get('@graph', []):
                        if entity['@id'] == id_uri:
                            id_value = process_identifier(entity)
                            if id_value:
                                identifiers.append(id_value)
    
    return f"{venue_title} [{' '.join(identifiers)}]" if identifiers else venue_title

def process_hierarchical_venue(entity: dict, rdf_dir: str, dir_split_number: int, items_per_file: int) -> Dict[str, str]:
    """Process hierarchical venue structure recursively"""
    result = {'volume': '', 'issue': '', 'venue': ''}
    entity_types = entity.get('@type', [])
    
    if 'http://purl.org/spar/fabio/JournalIssue' in entity_types:
        result['issue'] = entity.get('http://purl.org/spar/fabio/hasSequenceIdentifier', [{}])[0].get('@value', '')
    elif 'http://purl.org/spar/fabio/JournalVolume' in entity_types:
        result['volume'] = entity.get('http://purl.org/spar/fabio/hasSequenceIdentifier', [{}])[0].get('@value', '')
    else:
        # It's a main venue (journal, book series, etc.)
        result['venue'] = process_venue_title(entity, entity['@id'], rdf_dir, dir_split_number, items_per_file)
        return result

    # Process parent if exists
    if 'http://purl.org/vocab/frbr/core#partOf' in entity:
        parent_uri = entity['http://purl.org/vocab/frbr/core#partOf'][0]['@id']
        parent_file = find_file(rdf_dir, dir_split_number, items_per_file, parent_uri)
        if parent_file:
            parent_data = load_json_from_file(parent_file)
            for graph in parent_data:
                for parent_entity in graph.get('@graph', []):
                    if parent_entity['@id'] == parent_uri:
                        parent_info = process_hierarchical_venue(parent_entity, rdf_dir, dir_split_number, items_per_file)
                        # Merge parent info, keeping our current values
                        for key, value in parent_info.items():
                            if not result[key]:  # Only update if we don't have a value
                                result[key] = value
    
    return result

def process_bibliographic_resource(br_data: dict, rdf_dir: str, dir_split_number: int, items_per_file: int) -> Optional[Dict[str, str]]:
    """Process bibliographic resource data and its related entities"""
    # Skip if the entity is a JournalVolume or JournalIssue
    br_types = br_data.get('@type', [])
    if ('http://purl.org/spar/fabio/JournalVolume' in br_types or 
        'http://purl.org/spar/fabio/JournalIssue' in br_types):
        return None
        
    output = {field: '' for field in FIELDNAMES}
    
    try:
        # Extract OMID and basic BR information
        entity_id = br_data.get('@id', '')
        identifiers = [f'omid:br/{entity_id.split("/")[-1]}'] if entity_id else []
        
        output['title'] = br_data.get('http://purl.org/dc/terms/title', [{}])[0].get('@value', '')
        output['pub_date'] = br_data.get('http://prismstandard.org/namespaces/basic/2.0/publicationDate', [{}])[0].get('@value', '')
        
        # Extract type
        br_types = [t for t in br_data.get('@type', []) if t != 'http://purl.org/spar/fabio/Expression']
        output['type'] = URI_TYPE_DICT.get(br_types[0], '') if br_types else ''
        
        # Process identifiers
        if 'http://purl.org/spar/datacite/hasIdentifier' in br_data:
            for identifier in br_data['http://purl.org/spar/datacite/hasIdentifier']:
                id_uri = identifier['@id']
                id_file = find_file(rdf_dir, dir_split_number, items_per_file, id_uri)
                if id_file:
                    id_data = load_json_from_file(id_file)
                    for graph in id_data:
                        for entity in graph.get('@graph', []):
                            if entity['@id'] == id_uri:
                                id_value = process_identifier(entity)
                                if id_value:
                                    identifiers.append(id_value)
        output['id'] = ' '.join(identifiers)
        
        # Process authors, editors and publishers through agent roles
        authors = []
        editors = []
        publishers = []
        
        # Create a dictionary to store agent roles and their next relations
        agent_roles = {}
        next_relations = {}
        
        if 'http://purl.org/spar/pro/isDocumentContextFor' in br_data:
            # First pass: collect all agent roles and their next relations
            for ar_data in br_data['http://purl.org/spar/pro/isDocumentContextFor']:
                ar_uri = ar_data['@id']
                ar_file = find_file(rdf_dir, dir_split_number, items_per_file, ar_uri)
                if ar_file:
                    ar_data = load_json_from_file(ar_file)
                    for graph in ar_data:
                        for entity in graph.get('@graph', []):
                            if entity['@id'] == ar_uri:
                                # Store the agent role data
                                agent_roles[ar_uri] = entity
                                # Store the next relation if it exists
                                if 'https://w3id.org/oc/ontology/hasNext' in entity:
                                    next_ar = entity['https://w3id.org/oc/ontology/hasNext'][0]['@id']
                                    next_relations[ar_uri] = next_ar

            # Find the first agent role (the one that isn't referenced as next by any other)
            referenced_ars = set(next_relations.values())
            first_ar = None
            for ar_uri in agent_roles:
                if ar_uri not in referenced_ars:
                    first_ar = ar_uri
                    break
            
            # If no first AR found (in case of a complete cycle), take the first one from isDocumentContextFor
            if first_ar is None and 'http://purl.org/spar/pro/isDocumentContextFor' in br_data:
                first_ar = br_data['http://purl.org/spar/pro/isDocumentContextFor'][0]['@id']

            # Process agent roles in order
            current_ar = first_ar
            processed_ars = set()  # Keep track of processed ARs to detect loops
            max_iterations = len(agent_roles)  # Maximum number of iterations = number of ARs
            iterations = 0
            
            while current_ar and current_ar in agent_roles:
                # Check for cycles or too many iterations before processing
                if current_ar in processed_ars or iterations >= max_iterations:
                    print(f"Warning: Detected cycle in hasNext relations or exceeded maximum iterations at AR: {current_ar}")
                    break
                    
                processed_ars.add(current_ar)
                iterations += 1
                
                # Process the current AR
                entity = agent_roles[current_ar]
                role = entity.get('http://purl.org/spar/pro/withRole', [{}])[0].get('@id', '')
                
                if 'http://purl.org/spar/pro/isHeldBy' in entity:
                    ra_uri = entity['http://purl.org/spar/pro/isHeldBy'][0]['@id']
                    ra_file = find_file(rdf_dir, dir_split_number, items_per_file, ra_uri)
                    if ra_file:
                        ra_data = load_json_from_file(ra_file)
                        for ra_graph in ra_data:
                            for ra_entity in ra_graph.get('@graph', []):
                                if ra_entity['@id'] == ra_uri:
                                    agent_name = process_responsible_agent(ra_entity, ra_uri, rdf_dir, dir_split_number, items_per_file)
                                    if agent_name:
                                        if 'author' in role:
                                            authors.append(agent_name)
                                        elif 'editor' in role:
                                            editors.append(agent_name)
                                        elif 'publisher' in role:
                                            publishers.append(agent_name)
                
                # Move to next agent role
                current_ar = next_relations.get(current_ar)

            output['author'] = '; '.join(authors)
            output['editor'] = '; '.join(editors)
            output['publisher'] = '; '.join(publishers)
        
        # Process venue information
        if 'http://purl.org/vocab/frbr/core#partOf' in br_data:
            venue_uri = br_data['http://purl.org/vocab/frbr/core#partOf'][0]['@id']
            venue_file = find_file(rdf_dir, dir_split_number, items_per_file, venue_uri)
            if venue_file:
                venue_data = load_json_from_file(venue_file)
                for graph in venue_data:
                    for entity in graph.get('@graph', []):
                        if entity['@id'] == venue_uri:
                            venue_info = process_hierarchical_venue(entity, rdf_dir, dir_split_number, items_per_file)
                            output.update(venue_info)

        # Process page information
        if 'http://purl.org/vocab/frbr/core#embodiment' in br_data:
            page_uri = br_data['http://purl.org/vocab/frbr/core#embodiment'][0]['@id']
            page_file = find_file(rdf_dir, dir_split_number, items_per_file, page_uri)
            if page_file:
                page_data = load_json_from_file(page_file)
                for graph in page_data:
                    for entity in graph.get('@graph', []):
                        if entity['@id'] == page_uri:
                            start_page = entity.get('http://prismstandard.org/namespaces/basic/2.0/startingPage', [{}])[0].get('@value', '')
                            end_page = entity.get('http://prismstandard.org/namespaces/basic/2.0/endingPage', [{}])[0].get('@value', '')
                            if start_page or end_page:
                                output['page'] = f"{start_page}-{end_page}"
                            
    except (KeyError, IndexError) as e:
        print(f"Error processing bibliographic resource: {e}")
    
    return output

def process_file_chunk(args):
    """Process a chunk of files and return the results"""
    files, input_dir, dir_split_number, items_per_file, chunk_id = args
    results = []
    
    for filepath in files:
        data = load_json_from_file(filepath)
        for graph in data:
            for entity in graph.get('@graph', []):
                br_data = process_bibliographic_resource(entity, input_dir, dir_split_number, items_per_file)
                if br_data:
                    results.append(br_data)
    
    return results

def write_chunk_results(chunk_results, output_dir, chunk_id):
    """Write results from a chunk to a CSV file"""
    if chunk_results:
        output_file = os.path.join(output_dir, f'output_{chunk_id}.csv')
        write_csv(output_file, chunk_results)

def generate_csv(input_dir: str, output_dir: str, dir_split_number: int, items_per_file: int, zip_output_rdf: bool) -> None:
    """Generate CSV files from RDF data using multiprocessing"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process only files in the 'br' directory
    br_dir = os.path.join(input_dir, 'br')
    if not os.path.exists(br_dir):
        print(f"Error: bibliographic resources directory not found at {br_dir}")
        return
    
    # Collect all ZIP files first
    all_files = []
    for root, _, files in os.walk(br_dir):
        if 'prov' in root:
            continue
        all_files.extend(
            os.path.join(root, f) for f in files 
            if f.endswith('.zip')
        )
    
    # If no files found, return early
    if not all_files:
        print("No files found to process")
        return
    
    # Calculate optimal chunk size based on number of CPU cores
    num_cores = os.cpu_count()
    chunk_size = max(1, math.ceil(len(all_files) / (num_cores * 2)))  # Ensure minimum chunk size of 1
    file_chunks = [
        all_files[i:i + chunk_size] 
        for i in range(0, len(all_files), chunk_size)
    ]
    
    print(f"Processing {len(all_files)} files using {num_cores} cores...")
    
    # Prepare arguments for each chunk
    chunk_args = [
        (chunk, input_dir, dir_split_number, items_per_file, i) 
        for i, chunk in enumerate(file_chunks)
    ]
    
    # Process chunks in parallel
    with Pool() as pool:
        with tqdm(total=len(file_chunks), desc="Processing chunks") as pbar:
            for i, chunk_results in enumerate(pool.imap_unordered(process_file_chunk, chunk_args)):
                write_chunk_results(chunk_results, output_dir, i)
                pbar.update(1)

def write_csv(filepath: str, data: List[Dict[str, str]]) -> None:
    """Write data to CSV file"""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(data) 