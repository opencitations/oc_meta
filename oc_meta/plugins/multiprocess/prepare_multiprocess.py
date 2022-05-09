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


from oc_meta.lib.file_manager import pathoo, get_data, write_csv, sort_files
from oc_meta.lib.master_of_regex import comma_and_spaces, name_and_ids, semicolon_in_people_field
from oc_meta.scripts.creator import Creator
from typing import Dict, List
from tqdm import tqdm
import os
import psutil
import re


FORBIDDEN_IDS = {'issn:0000-0000'}
VENUES = {'archival-document', 'book', 'book-part', 'book-section', 'book-series', 'book-set', 'edited-book', 'journal', 'journal-volume', 'journal-issue', 'monograph', 'proceedings-series', 'proceedings', 'reference-book', 'report-series', 'standard-series'}

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
    files = [os.path.join(csv_dir, file) for file in sort_files(os.listdir(csv_dir)) if file.endswith('.csv')]
    pbar = tqdm(total=len(files)) if verbose else None
    pathoo(output_dir)
    ids_found = set()
    venues_found = dict()
    duplicated_ids = dict()
    venues_by_id = dict()
    publishers_found = set()
    publishers_by_id = dict()
    resp_agents_found = set()
    resp_agents_by_id = dict()
    # Look for all venues, responsible agents, and publishers
    for file in files:
        data = get_data(file)
        _get_duplicated_ids(data=data, ids_found=ids_found, items_by_id=duplicated_ids)
        _get_relevant_venues(data=data, ids_found=venues_found, items_by_id=venues_by_id)
        _get_publishers(data=data, ids_found=publishers_found, items_by_id=publishers_by_id)
        _get_resp_agents(data=data, ids_found=resp_agents_found, items_by_id=resp_agents_by_id)
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    ids_merged = _do_collective_merge(duplicated_ids)
    venues_merged = _do_collective_merge(venues_by_id)
    publishers_merged = _do_collective_merge(publishers_by_id)
    resp_agents_merged = _do_collective_merge(resp_agents_by_id)
    fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
    __save_relevant_venues(venues_merged, items_per_file, output_dir, fieldnames)
    __save_ids(ids_merged, items_per_file, output_dir, fieldnames)
    __save_responsible_agents(resp_agents_merged, items_per_file, output_dir, fieldnames, ('people', 'author'))
    __save_responsible_agents(publishers_merged, items_per_file, output_dir, fieldnames, ('publishers', 'publisher'))

def _get_duplicated_ids(data:List[dict], ids_found:set, items_by_id:Dict[str, dict]) -> None:
    cur_file_ids = set()
    for row in data:
        ids_list = row['id'].split()
        if any(id in ids_found and (id not in cur_file_ids or id in items_by_id) for id in ids_list):
            for id in ids_list:
                items_by_id.setdefault(id, {'others': set()})
                items_by_id[id]['others'].update({other for other in ids_list if other != id})
                for field in ['title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']:
                    if field in items_by_id[id]:
                        if items_by_id[id][field]:
                            continue
                    items_by_id[id][field] = row[field]
        cur_file_ids.update(set(ids_list))   
        ids_found.update(set(ids_list))

def _get_relevant_venues(data:List[dict], ids_found:dict, items_by_id:Dict[str, dict]) -> None:
    cur_file_ids = dict()
    for row in data:
        venue = row['venue']
        venues = list()
        if row['type'] in VENUES:
            venues.append((row['title'], row['id'], row['type']))
        if venue:
            full_name_and_ids = re.search(name_and_ids, venue)
            name = full_name_and_ids.group(1) if full_name_and_ids else venue
            ids = full_name_and_ids.group(2) if full_name_and_ids else None
            if ids:
                try:
                    br_type = Creator.get_venue_type(row['type'], ids.split())
                except UnboundLocalError:
                    print(f"[INFO:prepare_multiprocess] I found the venue {row['venue']} for the resource of type {row['type']}, but I don't know how to handle it")
                    raise UnboundLocalError
                venues.append((name, ids, br_type))
        for venue_tuple in venues:
            name, ids, br_type = venue_tuple
            ids_list = [identifier for identifier in ids.split() if identifier not in FORBIDDEN_IDS]
            if any(id in ids_found and (id not in cur_file_ids or id in items_by_id) for id in ids_list):
                for id in ids_list:
                    items_by_id.setdefault(id, {'others': set(), 'name': name, 'type': br_type, 'volume': dict(), 'issue': set()})
                    items_by_id[id]['others'].update({other for other in ids_list if other != id})
            for id in ids_list:
                ids_found.setdefault(id, {'volumes': dict(), 'issues': set()})
                cur_file_ids.setdefault(id, {'volumes': dict(), 'issues': set()})
            volume = row['volume']
            issue = row['issue']
            if volume:
                if any(volume in ids_found[id]['volumes'] and volume not in cur_file_ids[id]['volumes'] for id in ids_list):
                    for id in ids_list:
                        items_by_id[id]['volume'].setdefault(volume, set())
                for id in ids_list:
                    ids_found[id]['volumes'].setdefault(volume, set())
                    cur_file_ids[id]['volumes'].setdefault(volume, set())                        
                if issue:
                    if any(issue in ids_found[id]['volumes'][volume] and issue not in cur_file_ids[id]['volumes'][volume] for id in ids_list):
                        for id in ids_list:
                            items_by_id[id]['volume'].setdefault(volume, set())
                            items_by_id[id]['volume'][volume].add(issue)
                    for id in ids_list:
                        cur_file_ids[id]['volumes'][volume].add(issue)
                        ids_found[id]['volumes'][volume].add(issue)
            elif not volume and issue:
                if any(issue in ids_found[id]['issues'] and issue not in cur_file_ids[id]['issues'] for id in ids_list):
                    for id in ids_list:
                        items_by_id[id]['issue'].add(issue)
                for id in ids_list:
                    ids_found[id]['issues'].add(row['issue'])
                    cur_file_ids[id]['issues'].add(row['issue'])
                

def _get_publishers(data:List[dict], ids_found:set, items_by_id:Dict[str, dict]) -> None:
    cur_file_ids = set()
    for row in data:
        pub_name_and_ids = re.search(name_and_ids, row['publisher'])
        if pub_name_and_ids:
            pub_name = pub_name_and_ids.group(1)
            pub_ids = pub_name_and_ids.group(2).split()
            if any(id in ids_found and (id not in cur_file_ids or id in items_by_id) for id in pub_ids):
                for pub_id in pub_ids:
                    items_by_id.setdefault(pub_id, {'others': set(), 'name': pub_name, 'type': 'publisher'})
                    items_by_id[pub_id]['others'].update({other for other in pub_ids if other != pub_id})
            cur_file_ids.update(set(pub_ids))   
            ids_found.update(set(pub_ids))

def _get_resp_agents(data:List[dict], ids_found:set, items_by_id:Dict[str, Dict[str, set]]) -> None:
    cur_file_ids = set()
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
                        richest_name = _find_all_names(items_by_id, ids_list, name)
                        if any(id in ids_found and (id not in cur_file_ids or id in items_by_id) for id in ids_list):
                            for id in ids_list:
                                items_by_id.setdefault(id, {'others': set(), 'type': 'author'})
                                items_by_id[id]['name'] = richest_name
                                items_by_id[id]['others'].update({other for other in ids_list if other != id})
                        cur_file_ids.update(set(ids_list))   
                        ids_found.update(set(ids_list))

def _find_all_names(items_by_id:Dict[str, Dict[str, set]], ids_list:list, cur_name:str) -> str:
    all_names = {cur_name}
    for id in ids_list:
        if id in items_by_id:
            all_names.add(items_by_id[id]['name'])
    all_names_parsed = set()
    for name in all_names:
        if ',' in name:
            split_name = re.split(comma_and_spaces, name)
            first_name = split_name[1].strip()
            surname = split_name[0].strip()
            all_names_parsed.add((surname, first_name))
        else:
            all_names_parsed.add((name,))
    richest_first_name = ''
    richest_surname = ''
    for name in all_names_parsed:
        if name[0] > richest_surname:
            richest_surname = name[0]
        if len(name) == 2:
            if name[1] > richest_first_name:
                richest_first_name = name[1]
    return f'{richest_surname}, {richest_first_name}'.strip()

def _do_collective_merge(items_by_id:dict) -> dict:
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
                if isinstance(data['volume'], dict):
                    all_vi = __find_all_vi(items_by_id=items_by_id, all_ids=ids_found)
            if data['type'] == 'author':
                richest_name = _find_all_names(items_by_id, all_ids, data['name'])
                data['name'] = richest_name
            merged_by_key[id] = {k:v if not k == 'others' else all_ids for k,v in data.items()}
            if all_vi:
                merged_by_key[id]['volume'] = all_vi['volume']
                merged_by_key[id]['issue'] = all_vi['issue']
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
    counter = 0
    for item_id, data in items_by_id.items():
        item_type = data['type']
        row = dict()
        name, ids = __get_name_and_ids(item_id, data)
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
            else:
                volume_row['type'] = 'journal volume'
                rows.append(volume_row)
        for venue_issue in data['issue']:
            issue_row = dict()
            issue_row['venue'] = f'{name} [{ids}]'
            issue_row['issue'] = venue_issue
            issue_row['type'] = 'journal issue'
            rows.append(issue_row)
        if not data['volume'] and not data['issue']:
            rows.append(row)
        if len(rows) >= items_per_file:
            output_path = os.path.join(output_dir, f"{counter}.csv")
            write_csv(output_path, rows, fieldnames)
            rows = list()
            counter += 1
    output_path = os.path.join(output_dir, f"{counter}.csv")
    write_csv(output_path, rows, fieldnames)

def __save_responsible_agents(items_by_id:dict, items_per_file:int, output_dir:str, fieldnames:list, datatype:tuple):
    folder = datatype[0]
    field = datatype[1]
    output_dir = os.path.join(output_dir, folder)
    rows = list()
    chunks = int(items_per_file)
    saved_chunks = 0
    output_length = len(items_by_id)
    for item_id, data in items_by_id.items():
        name, ids = __get_name_and_ids(item_id, data)
        rows.append({field: f'{name} [{ids}]'})
        rows, saved_chunks = __store_data(rows, output_length, chunks, saved_chunks, output_dir, fieldnames)

def __save_ids(items_by_id:dict, items_per_file:int, output_dir:str, fieldnames:list):
    output_dir = os.path.join(output_dir, 'ids')
    rows = list()
    chunks = int(items_per_file)
    saved_chunks = 0
    output_length = len(items_by_id)
    for item_id, data in items_by_id.items():
        ids_list = list(data['others'])
        ids_list.append(item_id)
        ids = ' '.join(ids_list)
        output_row = {'id': ids}
        for field in [f for f in fieldnames if f != 'id']:
            output_row[field] = data[field]
        rows.append(output_row)
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

def __get_name_and_ids(item_id:str, data:dict):
    ids_list = list(data['others'])
    ids_list.append(item_id)
    name = data['name'] if 'name' in data else ''
    ids = ' '.join(ids_list)
    return name, ids

def split_csvs_in_chunks(csv_dir:str, output_dir:str, chunk_size:int, verbose:bool=False) -> None:
    '''
    This function splits all CSVs in a folder in smaller CSVs having a specified number of rows.
    Moreover, this function tries, where possible, to keep in a single file the bibliographic resources contained in the same venue. 
    For this reason, the final rows number could be slightly over the specified one.

    :params csv_dir: the path to the folder containing the input CSV files
    :type csv_dir: str
    :params output_dir: the location of the folder to save to output files
    :type output_dir: str
    :params chunk_size: an integer to specify how many rows to insert in each output file
    :type chunk_size: int
    :params verbose: if True, show a loading bar, elapsed, and estimated time
    :type verbose: bool
    :returns: None -- This function returns None and saves the output CSV files in the `output_dir` folder
    '''
    files = [os.path.join(csv_dir, file) for file in sort_files(os.listdir(csv_dir)) if file.endswith('.csv')]
    pid = psutil.Process(os.getpid())
    venues_occurrences = __index_all_venues(files, verbose)
    __split_csvs_by_venues(files, venues_occurrences, output_dir, pid, verbose)
    __split_in_chunks(output_dir, chunk_size, verbose)

def __index_all_venues(files:list, verbose:bool) -> dict:
    if verbose:
        print('[INFO:prepare_multiprocess] Scanning venues')
        pbar = tqdm(total=len(files))
    venues_occurrences = dict()
    for file in files:
        data = get_data(file)
        for row in data:
            venues = list()
            if row['type'] in VENUES:
                venues.append(row['id'].split())
            venue_and_ids = re.search(name_and_ids, row['venue'])
            if venue_and_ids:
                ids = venue_and_ids.group(2).split()
                venues.append(ids)
            for venue_ids in venues:
                for venue_id in venue_ids:
                    venues_occurrences.setdefault(venue_id, {'others': set()})
                    venues_occurrences[venue_id]['others'].update({other for other in venue_ids if other != venue_id})
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    return venues_occurrences

def __split_csvs_by_venues(files:list, venues_occurrences:dict, output_dir:str, pid:psutil.Process, verbose:bool):
    pathoo(output_dir)
    if verbose:
        print('[INFO:prepare_multiprocess] Splitting CSVs by venue')
        pbar = tqdm(total=len(files))
    chunk_venues = dict()
    chunk_no_venues = dict()
    existing_files = set()
    no_venues_outdata = list()
    counter = 0
    for file in files:
        data = get_data(file)
        for row in data:
            venues = list()
            if row['type'] in VENUES:
                venues.append(row['id'].split())
            venue_and_ids = re.search(name_and_ids, row['venue'])
            if venue_and_ids:
                ids = venue_and_ids.group(2).split()
                venues.append(ids)
            if venues:
                output_filepath = None
                for venue_ids in venues:
                    all_ids:list = venue_ids
                    all_ids.extend(__find_all_ids_by_key(venues_occurrences, key=all_ids[0]))
                    for any_id in all_ids:
                        filename = any_id.replace(':', '').replace('/', '').replace('\\', '')
                        if os.path.join(output_dir, f'{filename}.csv') in existing_files:
                            output_filepath = os.path.join(output_dir, f'{filename}.csv')
                filename = all_ids[0].replace(':', '').replace('/', '').replace('\\', '')
                output_filepath = os.path.join(output_dir, f'{filename}.csv') if not output_filepath else output_filepath
                chunk_venues.setdefault(output_filepath, list()).append(row)
                chunk_venues = __dump_if_chunk_size(chunk_venues, existing_files, pid)
            elif not venues:
                no_venues_outdata.append(row)
                if len(no_venues_outdata) == 1000:
                    no_venues_filepath = os.path.join(output_dir, f'no_venues_{counter}.csv')
                    chunk_no_venues[no_venues_filepath] = no_venues_outdata
                    counter += 1
                    no_venues_outdata = list()
                chunk_no_venues = __dump_if_chunk_size(chunk_no_venues, existing_files, pid)
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    if no_venues_outdata:
        no_venues_filepath = os.path.join(output_dir, f'no_venues_{counter}.csv')
        chunk_no_venues[no_venues_filepath] = no_venues_outdata
    for chunk in [chunk_venues, chunk_no_venues]:
        for filepath, dump in chunk.items():
            all_data = get_data(filepath) if os.path.exists(filepath) else list()
            all_data.extend(dump)
            write_csv(filepath, all_data)
        del chunk

def __split_in_chunks(output_dir:str, chunk_size:int, verbose:bool):
    files = os.listdir(output_dir)
    if verbose:
        print('[INFO:prepare_multiprocess] Splitting CSVs in chunks')
        pbar = tqdm(total=len(files))
    even_chunk = list()
    counter = 0
    for file in files:
        filepath = os.path.join(output_dir, file)
        data = get_data(filepath)
        len_data = len(data)
        if len_data > chunk_size:
            while len_data > chunk_size:
                write_csv(os.path.join(output_dir, f'{counter}.csv'), data[:chunk_size])
                counter += 1
                del data[:chunk_size]
                len_data = len(data)
            even_chunk.extend(data)
            if len(even_chunk) >= chunk_size:
                write_csv(os.path.join(output_dir, f'{counter}.csv'), even_chunk)
                counter += 1
                even_chunk = list()
        elif len_data <= chunk_size:
            even_chunk.extend(data)
            if len(even_chunk) >= chunk_size:
                write_csv(os.path.join(output_dir, f'{counter}.csv'), even_chunk)
                counter += 1
                even_chunk = list()
        os.remove(filepath)
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    if even_chunk:
        write_csv(os.path.join(output_dir, f'{counter}.csv'), even_chunk)

def __dump_if_chunk_size(chunk:dict, existing_files:set, pid:psutil.Process) -> dict:
    memory_used = pid.memory_info().rss / (1024.0 ** 3)
    if memory_used > 10:
        for filepath, dump in chunk.items():
            all_data = get_data(filepath) if os.path.exists(filepath) else list()
            all_data.extend(dump)
            write_csv(filepath, all_data)
            existing_files.add(filepath)
        return dict()
    return chunk

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
