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


import chunk
from meta.lib.file_manager import pathoo, get_data, write_csv, sort_files
from meta.lib.master_of_regex import name_and_ids, semicolon_in_people_field
from meta.scripts.creator import Creator
from typing import Dict, List
from tqdm import tqdm
import csv
import os
import re


VENUES = {'archival-document', 'book', 'book-part', 'book-section', 'book-series', 'book-set', 'edited-book', 'journal', 'journal-volume', 'journal-issue', 'monograph', 'proceedings-series', 'proceedings', 'reference-book', 'report-series', 'standard-series'}


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
    venues_occurrences = __index_all_venues(files, verbose)
    __split_csvs_by_venues(files, venues_occurrences, output_dir, verbose)
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

def __split_csvs_by_venues(files:list, venues_occurrences:dict, output_dir:str, verbose:bool):
    pathoo(output_dir)
    if verbose:
        print('[INFO:prepare_multiprocess] Splitting CSVs by venue')
        pbar = tqdm(total=len(files))
    chunk_venues = dict()
    chunk_no_venues = dict()
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
                for venue_ids in venues:
                    all_ids:list = venue_ids
                    all_ids.extend(__find_all_ids_by_key(venues_occurrences, key=all_ids[0]))
                    output_filepath = None
                    for any_id in all_ids:
                        filename = any_id.replace(':', '').replace('/', '').replace('\\', '')
                        if os.path.exists(os.path.join(output_dir, f'{filename}.csv')):
                            output_filepath = os.path.join(output_dir, f'{filename}.csv')
                    filename = all_ids[0].replace(':', '').replace('/', '').replace('\\', '')
                    output_filepath = os.path.join(output_dir, f'{filename}.csv') if not output_filepath else output_filepath
                    chunk_venues.setdefault(output_filepath, list()).append(row)
                    __dump_if_chunk_size(chunk_venues)
            elif not venues:
                no_venues_file = os.path.join(output_dir, 'no_venues.csv')
                chunk_no_venues.setdefault(no_venues_file, list()).append(row)
                __dump_if_chunk_size(chunk_no_venues)
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    for chunk in [chunk_venues, chunk_no_venues]:
        for filepath, dump in chunk.items():
            all_data = get_data(filepath) if os.path.exists(filepath) else list()
            all_data.extend(dump)
            write_csv(filepath, all_data)

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
            while len_data:
                end = chunk_size if len_data > chunk_size else len_data
                write_csv(os.path.join(output_dir, f'{counter}.csv'), data[:end])
                counter += 1
                len_data -= end
        elif len_data <= chunk_size:
            even_chunk.extend(data)
            if len(even_chunk) > chunk_size:
                write_csv(os.path.join(output_dir, f'{counter}.csv'), even_chunk)
                counter += 1
                even_chunk = list()
        os.remove(filepath)
        pbar.update()
    pbar.close()
    write_csv(os.path.join(output_dir, f'{counter}.csv'), even_chunk)

def __dump_if_chunk_size(chunk:dict):
    if sum((len(v) for v in chunk.values())) > 1000000:
        for filepath, dump in chunk.items():
            all_data = get_data(filepath) if os.path.exists(filepath) else list()
            all_data.extend(dump)
            write_csv(filepath, all_data)
        chunk = dict()

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

if __name__ == '__main__':
    split_csvs_in_chunks(csv_dir='D:/meta_input_old', output_dir='C:/Users/arcangelo.massari2/Desktop/test_smart_split', chunk_size=1000, verbose=True)
