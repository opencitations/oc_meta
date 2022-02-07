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


import os, re
from typing import List, Dict
from meta.lib.file_manager import pathoo, get_data, write_csv
from meta.lib.master_of_regex import ids_inside_square_brackets, name_and_ids
from meta.lib.csvmanager import CSVManager
from tqdm import tqdm


def prepare_relevant_venues(csv_dir: str, output_dir: str, wanted_dois:str, items_per_file:int, verbose:bool=False) -> None:
    '''
    This function receives an input folder containing CSVs formatted for Meta. 
    It output other CSVs, including deduplicated venues only. 
    You can specify the list of desired DOIs and how many items to insert in each output file.

    :params csv_dir: the path to the folder containing the input CSV files
    :type csv_dir: str
    :params output_dir: the location of the folder to save to output file
    :type output_dir: str
    :params wanted_dois: the path of the CSV file containing the list of DOIs to consider
    :type wanted_dois: str
    :params items_per_file: an integer to specify how many rows to insert in each output file
    :type items_per_file: int
    :params verbose: if True, show a loading bar, elapsed, and estimated time
    :type verbose: bool
    :returns: None -- This function returns None and saves the output CSV files in the `output_dir` folder
    '''
    if verbose and wanted_dois:
        print('[INFO:prepare_multiprocess] Getting the wanted DOIs')
    doi_set = CSVManager.load_csv_column_as_set(wanted_dois, 'doi') if wanted_dois else None
    files = os.listdir(csv_dir)
    if verbose:
        pbar = tqdm(total=len(files))
    pathoo(output_dir)
    venue_by_id = dict()
    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(csv_dir, file)
            __get_relevant_venues(file_path=file_path, venue_by_id=venue_by_id, doi_set=doi_set)
        if verbose:
            pbar.update()
    if verbose:
        pbar.close()
    venue_merged = __do_collective_merge(venue_by_id, verbose)
    __save_relevant_venues(venue_merged, items_per_file, output_dir)          

def __get_relevant_venues(file_path:str, venue_by_id:dict, doi_set:set=None) -> None:
    data = get_data(file_path)
    for row in data:
        if row['venue']:
            venue_name_and_ids = re.search(name_and_ids, row['venue'])
            venue_name = venue_name_and_ids.group(1) if venue_name_and_ids else row['venue']
            venue_ids = venue_name_and_ids.group(2) if venue_name_and_ids else None
            relevant_doi = any(id.replace('doi:', '') in doi_set for id in row['id'].split()) if doi_set else True
            if venue_ids and relevant_doi:
                venue_ids_list = venue_ids.split()
                first_id = venue_ids_list[0]
                venue_by_id.setdefault(first_id, {'others': set(), 'name': venue_name})
                venue_by_id[first_id]['others'].update({id for id in venue_ids_list if id != first_id})

def __do_collective_merge(venue_by_id:dict, verbose:bool) -> dict:
    if verbose:
        print('[INFO:prepare_multiprocess] Merging the relevant venues found')
        pbar = tqdm(total=len(venue_by_id))
    venue_merged = dict()
    for id, data in venue_by_id.items():
        if id in venue_merged:
            key_to_update = id
        else:
            key_to_update = next((k for k,v in venue_merged.items() if id in v['others']), None)
        if key_to_update:
            venue_merged[key_to_update]['others'].update(data['others'])
        else:
            venue_merged[id] = data
        if verbose:
            pbar.update()
    if verbose:
        pbar.close()
    return venue_merged

def __save_relevant_venues(venue_by_id:dict, items_per_file:int, output_dir:str):
    fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
    rows = list()
    chunks = int(items_per_file)
    saved_chunks = 0
    output_length = len(venue_by_id)
    for venue_id, data in venue_by_id.items():
        row = dict()
        ids = list(data['others'])
        ids.append(venue_id)
        row['id'] = ' '.join(ids)
        row['title'] = data['name']
        row['type'] = 'journal'
        rows.append(row)
        data_about_to_end = (output_length - saved_chunks) < chunks
        if len(rows) == chunks or data_about_to_end:
            saved_chunks = saved_chunks + chunks if not data_about_to_end else output_length
            filename = f"{str(saved_chunks)}.csv"
            output_path = os.path.join(output_dir, filename)
            write_csv(path=output_path, datalist=rows, fieldnames=fieldnames)
            rows = list()

def split_by_publisher(csv_dir: str, output_dir: str, verbose:bool=False) -> None:
    '''
    This function receives an input folder containing CSVs formatted for Meta. 
    It output other CSVs divided by publisher. The output files names match the publishers’s ids and contain only documents published by that publisher.
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
        if file.endswith(".csv"):
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
