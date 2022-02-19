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


from concurrent.futures import ProcessPoolExecutor, as_completed
from meta.lib.file_manager import pathoo, get_data, write_csv
from meta.lib.master_of_regex import ids_inside_square_brackets, name_and_ids, semicolon_in_people_field
from meta.run.meta_process import MetaProcess
from SPARQLWrapper import SPARQLWrapper, POST
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
    items_by_id = dict()
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(csv_dir, file)
            data = get_data(file_path)
            __get_relevant_venues(data=data, items_by_id=items_by_id)
            __get_resp_agents(data=data, items_by_id=items_by_id)
        if verbose:
            pbar.update()
    if verbose:
        pbar.close()
    item_merged = _do_collective_merge(items_by_id, verbose)
    __save_relevant_items(item_merged, items_per_file, output_dir)
    del item_merged   

def __get_relevant_venues(data:List[dict], items_by_id:dict) -> None:
    for row in data:
        if row['venue']:
            _update_items_by_id(item=row['venue'], field='journal', items_by_id=items_by_id)

def __get_resp_agents(data:List[dict], items_by_id:Dict[str, Dict[str, set]]) -> None:
    for row in data:
        for field in {'author', 'editor'}:
            if row[field]:
                resp_agents = re.split(semicolon_in_people_field, row[field])
                for resp_agent in resp_agents:
                    # Whether the responsible agent is listed as an author or editor is not important. 
                    # In fact, the agent role will not be recorded on the triplestore, 
                    # but the only information registered will be the people and their identifier
                    _update_items_by_id(item=resp_agent, field='author', items_by_id=items_by_id)

def _update_items_by_id(item:str, field:str,  items_by_id:Dict[str, Dict[str, set]]) -> None:
    full_name_and_ids = re.search(name_and_ids, item)
    name = full_name_and_ids.group(1) if full_name_and_ids else item
    ids = full_name_and_ids.group(2) if full_name_and_ids else None
    if ids:
        ids_list = ids.split()
        for id in ids_list:
            items_by_id.setdefault(id, {'others': set(), 'name': name, 'type': field})
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
            all_ids.update(data['others'])
            for other in data['others']:
                if other not in ids_checked:
                    ids_found = __find_all_ids_by_key(items_by_id, key=other)
                    all_ids.update({item for item in ids_found if item != id})
                    ids_checked.update(ids_found)
            merged_by_key[id] = {'name': data['name'], 'type': data['type'], 'others': all_ids}
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

def __save_relevant_items(items_by_id:dict, items_per_file:int, output_dir:str):
    fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
    rows = list()
    chunks = int(items_per_file)
    saved_chunks = 0
    output_length = len(items_by_id)
    for item_id, data in items_by_id.items():
        item_type = data['type']
        row = dict()
        ids_list = list(data['others'])
        ids_list.append(item_id)
        name = data['name']
        ids = ' '.join(ids_list)
        if item_type == 'journal':
            row['id'] = ids
            row['title'] = name
            row['type'] = item_type
        elif item_type == 'author':
            row[item_type] = f'{name} [{ids}]'
        rows.append(row)
        data_about_to_end = (output_length - saved_chunks) < chunks and (output_length - saved_chunks) == len(rows)
        if len(rows) == chunks or data_about_to_end:
            saved_chunks = saved_chunks + chunks if not data_about_to_end else output_length
            filename = f'{str(saved_chunks)}.csv'
            output_path = os.path.join(output_dir, filename)
            write_csv(path=output_path, datalist=rows, fieldnames=fieldnames)
            rows = list()

def delete_unwanted_statements(meta_process:MetaProcess):
    indexes_dir = meta_process.indexes_dir
    files = [os.path.join(fold,file) for fold, _, files in os.walk(indexes_dir) for file in files if file == 'index_ar.csv']
    verbose = meta_process.verbose
    if verbose:
        print('[INFO:prepare_multiprocess] Deleting unwanted statements from the triplestore')
        pbar = tqdm(total=len(files))
    base_iri = meta_process.base_iri[:-1] if meta_process.base_iri[-1] == '/' else meta_process.base_iri
    triplestore_url = meta_process.triplestore_url
    with ProcessPoolExecutor(meta_process.workers_number) as executor:
        results = [executor.submit(__submit_delete_query, file_path, triplestore_url, base_iri) for file_path in files]
        for _ in as_completed(results):
            pbar.update() if verbose else None
    pbar.close() if verbose else None
    
def __submit_delete_query(file_path:str, triplestore_url:str, base_iri:str):
    sparql = SPARQLWrapper(triplestore_url)
    sparql.setMethod(POST)
    data = get_data(file_path)
    for row in data:
        br_metaid = row['meta']
        ar_metaid = row['author'].split(', ')[0]
        query = f'''
            DELETE {{
                ?s ?p ?o.
            }}
            WHERE {{
                ?s ?p ?o.
                VALUES ?s {{
                    <{base_iri}/br/{br_metaid}>
                    <{base_iri}/ar/{ar_metaid}>
                }}
            }}
        '''
        sparql.setQuery(query)
        sparql.query()

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