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


from meta.lib.file_manager import pathoo, get_data, write_csv
from meta.lib.master_of_regex import ids_inside_square_brackets, name_and_ids, semicolon_in_people_field
from typing import List, Dict
from tqdm import tqdm
import os
import re


FORBIDDEN_IDS = {'issn:0000-0000'}

def prepare_relevant_items(csv_dir:str, output_dir:str, items_per_file:int, verbose:bool) -> None:
    '''
    This function receives an input folder containing CSVs formatted for Meta. 
    It output other CSVs, including deduplicated items only. 
    You can specify how many items to insert in each output file.

    :params csv_dir: the path to the folder containing the input CSV files
    :type csv_dir: str
    :params output_dir: the location of the folder to save to output file
    :type output_dir: str
    :params items_per_file: an integer to specify how many rows to insert in each output file
    :type items_per_file: int
    :params verbose: if True, show a loading bar, elapsed, and estimated time
    :type verbose: bool
    :returns: None -- This function returns None and saves the output CSV files in the `output_dir` folder
    '''
    files = os.listdir(csv_dir)
    if verbose:
        pbar = tqdm(total=len(files))
    pathoo(output_dir)
    venues_by_id = dict()
    resp_agents_by_id = dict()
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(csv_dir, file)
            data = get_data(file_path)
            _get_relevant_venues(data=data, items_by_id=venues_by_id)
            _get_resp_agents(data=data, items_by_id=resp_agents_by_id)
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    venues_merged = _do_collective_merge(venues_by_id, verbose)
    resp_agents_merged = _do_collective_merge(resp_agents_by_id, verbose)
    fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
    __save_relevant_venues(venues_merged, items_per_file, output_dir, fieldnames)
    __save_resp_agents(resp_agents_merged, items_per_file, output_dir, fieldnames)

def _get_relevant_venues(data:List[dict], items_by_id:Dict[str, dict]) -> None:
    for row in data:
        venue = row['venue']
        ids = None
        if venue:
            full_name_and_ids = re.search(name_and_ids, venue)
            name = full_name_and_ids.group(1) if full_name_and_ids else venue
            ids = full_name_and_ids.group(2) if full_name_and_ids else None
        elif row['type'] == 'journal':
            name = row['title']
            ids = row['id']
        if ids:
            ids_list = [identifier for identifier in ids.split() if identifier not in FORBIDDEN_IDS]
            for id in ids_list:
                items_by_id.setdefault(id, {'others': set(), 'name': name, 'type': 'journal', 'volume': dict(), 'issue': set()})
                items_by_id[id]['others'].update({other for other in ids_list if other != id})
                volume = row['volume']
                issue = row['issue']
                if volume:
                    items_by_id[id]['volume'].setdefault(volume, set())
                    if issue:
                        items_by_id[id]['volume'][volume].add(issue)
                elif not volume and issue:
                    items_by_id[id]['issue'].add(issue)

def _get_resp_agents(data:List[dict], items_by_id:Dict[str, Dict[str, set]]) -> None:
    for row in data:
        for field in {'author', 'editor'}:
            if row[field]:
                resp_agents = re.split(semicolon_in_people_field, row[field])
                for resp_agent in resp_agents:
                    full_name_and_ids = re.search(name_and_ids, resp_agent)
                    name = full_name_and_ids.group(1) if full_name_and_ids else resp_agent
                    ids = full_name_and_ids.group(2) if full_name_and_ids else None
                    if ids:
                        ids_list = [identifier for identifier in ids.split() if identifier not in FORBIDDEN_IDS]
                        for id in ids_list:
                            items_by_id.setdefault(id, {'others': set(), 'name': name, 'type': 'author'})
                            items_by_id[id]['others'].update({other for other in ids_list if other != id})

def _do_collective_merge(items_by_id:dict, verbose:bool=False) -> dict:
    if verbose:
        print('[INFO:prepare_multiprocess] Merging the relevant items found')
        pbar = tqdm(total=len(items_by_id))
    merged_by_key:Dict[str, Dict[str, set]] = dict()
    ids_checked = set()
    for id, data in items_by_id.items():
        if id not in ids_checked:
            all_ids = set()
            all_vi = None
            if data['others']:
                all_ids.update(data['others'])
                for other in data['others']:
                    if other not in ids_checked:
                        ids_found = __find_all_ids_by_key(items_by_id, key=other)
                        all_ids.update({item for item in ids_found if item != id})
                        ids_checked.update(ids_found)
            else:
                ids_found = {id}
            if 'volume' in data and 'issue' in data:
                all_vi = __find_all_vi(items_by_id=items_by_id, all_ids=ids_found)                        
            merged_by_key[id] = {'name': data['name'], 'type': data['type'], 'others': all_ids}
            if all_vi:
                merged_by_key[id]['volume'] = all_vi['volume']
                merged_by_key[id]['issue'] = all_vi['issue']
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    del items_by_id
    return merged_by_key

def __find_all_ids_by_key(items_by_id:dict, key:str):
    visited_items = set()
    items_to_visit = {item for item in items_by_id[key]['others']}
    while items_to_visit:
        for item in set(items_to_visit):
            if item not in visited_items:
                visited_items.add(item)
                items_to_visit.update({item for item in items_by_id[item]['others'] if item not in visited_items})
            items_to_visit.remove(item)
    return visited_items

def __find_all_vi(items_by_id:dict, all_ids:set) -> dict:
    all_vi = {'volume': dict(), 'issue': set()}
    for id in all_ids:
        for volume, volume_issues in items_by_id[id]['volume'].items():
            all_vi['volume'].setdefault(volume, set()).update(volume_issues)
        for venue_issues in items_by_id[id]['issue']:
            all_vi['issue'].add(venue_issues)
    return all_vi

def __save_relevant_venues(items_by_id:dict, items_per_file:int, output_dir:str, fieldnames:list):
    output_dir = os.path.join(output_dir, 'venues')
    rows = list()
    chunks = int(items_per_file)
    saved_chunks = 0
    output_length = 0
    for _, data in items_by_id.items():
        for _, issues in data['volume'].items():
            output_length = output_length + len(issues) if issues else output_length + 1
        output_length += len(data['issue'])
        if not data['volume'] and not data['issue']:
            output_length += 1
    for item_id, data in items_by_id.items():
        item_type = data['type']
        row = dict()
        name, ids = __get_name_and_ids(item_id, data)
        if item_type == 'journal':
            row['id'] = ids
            row['title'] = name
            row['type'] = item_type
            for volume, volume_issues in data['volume'].items():
                volume_row = dict()
                volume_row['volume'] = volume
                volume_row['venue'] = f'{name} [{ids}]'
                if volume_issues:
                    volume_row['type'] = 'journal issue'
                    for volume_issue in volume_issues:
                        volume_issue_row = dict(volume_row)
                        volume_issue_row['issue'] = volume_issue
                        rows.append(volume_issue_row)
                        rows, saved_chunks = __store_data(rows, output_length, chunks, saved_chunks, output_dir, fieldnames)
                else:
                    volume_row['type'] = 'journal volume'
                    rows.append(volume_row)
                    rows, saved_chunks = __store_data(rows, output_length, chunks, saved_chunks, output_dir, fieldnames)
            for venue_issue in data['issue']:
                issue_row = dict()
                issue_row['venue'] = f'{name} [{ids}]'
                issue_row['issue'] = venue_issue
                issue_row['type'] = 'journal issue'
                rows.append(issue_row)
                rows, saved_chunks = __store_data(rows, output_length, chunks, saved_chunks, output_dir, fieldnames)
            if not data['volume'] and not data['issue']:
                rows.append(row)
                rows, saved_chunks = __store_data(rows, output_length, chunks, saved_chunks, output_dir, fieldnames)

def __save_resp_agents(items_by_id:dict, items_per_file:int, output_dir:str, fieldnames:list):
    output_dir = os.path.join(output_dir, 'people')
    rows = list()
    chunks = int(items_per_file)
    saved_chunks = 0
    output_length = len(items_by_id)
    for item_id, data in items_by_id.items():
        name, ids = __get_name_and_ids(item_id, data)
        rows.append({'author': f'{name} [{ids}]'})
        rows, saved_chunks = __store_data(rows, output_length, chunks, saved_chunks, output_dir, fieldnames)

def __store_data(rows:list, output_length:int, chunks:int, saved_chunks:int, output_dir:str, fieldnames:str) -> list:
    data_about_to_end = (output_length - saved_chunks) < chunks and (output_length - saved_chunks) == len(rows)
    if len(rows) == chunks or data_about_to_end:
        saved_chunks = saved_chunks + chunks if not data_about_to_end else output_length
        filename = f'{str(saved_chunks)}.csv'
        output_path = os.path.join(output_dir, filename)
        write_csv(path=output_path, datalist=rows, fieldnames=fieldnames)
        rows = list()
    return rows, saved_chunks

def __get_name_and_ids(item_id, data):
    ids_list = list(data['others'])
    ids_list.append(item_id)
    name = data['name']
    ids = ' '.join(ids_list)
    return name, ids

def split_by_publisher(csv_dir:str, output_dir:str, verbose:bool=False) -> None:
    '''
    This function receives an input folder containing CSVs formatted for Meta. 
    It output other CSVs divided by publisher. The output files names match the publishers's ids and contain only documents published by that publisher.
    For example, a file containing documents published by the American Mathematical Society on Crossref will be called crossref_14.csv, because this publisher has id 14 on Crossref.

    :params csv_dir: the path to the folder containing the input CSV files
    :type csv_dir: str
    :params output_dir: the location of the folder to save to output file
    :type output_dir: str
    :params verbose: if True, show a loading bar, elapsed, and estimated time
    :type verbose: bool
    :returns: None -- This function returns None and saves the output CSV files in the `output_dir` folder
    '''
    files = os.listdir(csv_dir)
    pathoo(output_dir)
    if verbose:
        print('[INFO:prepare_multiprocess] Splitting CSVs by publishers')
        pbar = tqdm(total=len(files))
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(csv_dir, file)
            data = get_data(file_path)
            data_by_publisher:Dict[str, List] = dict()
            for row in data:
                if row['publisher']:
                    id = re.search(ids_inside_square_brackets, row['publisher'])
                    publisher = id.group(1) if id else row['publisher']
                    data_by_publisher.setdefault(publisher, list()).append(row)
            for publisher, data in data_by_publisher.items():
                publisher = publisher.replace(':', '_')
                publisher += '.csv'
                output_file_path = os.path.join(output_dir, publisher)
                write_csv(path=output_file_path, datalist=data)
        if verbose:
            pbar.update()
    if verbose:
        pbar.close()