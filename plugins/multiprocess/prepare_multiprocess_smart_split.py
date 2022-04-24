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


from meta.lib.file_manager import pathoo, get_data, write_csv, sort_files
from meta.lib.master_of_regex import name_and_ids, semicolon_in_people_field
from meta.scripts.creator import Creator
from typing import Dict, List
from tqdm import tqdm
import os
import re


def split_csvs_in_chunks(csv_dir:str, output_dir:str, chunk_size:int, verbose:bool=False) -> None:
    '''
    This function splits all CSVs in a folder in smaller CSVs having a specified number of rows.

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
    files = [os.path.join(csv_dir, file) for file in sort_files(os.listdir(csv_dir)) if file.endswith('.csv')][:1000]
    if verbose:
        print('[INFO:prepare_multiprocess] Scanning venues')
        pbar = tqdm(total=len(files))
    venues_occurrences = dict()
    for file in files:
        data = get_data(file)
        for i, row in enumerate(data):
            venue_and_ids = re.search(name_and_ids, row['venue'])
            if venue_and_ids:
                venue = venue_and_ids.group(1)
                ids = venue_and_ids.group(2).split()
                for id in ids:
                    venues_occurrences.setdefault(id, {'name': venue, 'others': set(), 'files': dict(), 'occurrences': 0})
                    venues_occurrences[id]['files'].setdefault(file, list())
                    venues_occurrences[id]['files'][file].append(i)
                    venues_occurrences[id]['occurrences'] += 1
                    venues_occurrences[id]['others'].update({other for other in ids if other != id})
        pbar.update() if verbose else None
    pbar.close() if verbose else None
    pathoo(output_dir)
    even_chunk = list()
    uneven_chunk = list()
    processed_rows = set()
    counter = 0
    if verbose:
        print('[INFO:prepare_multiprocess] Splitting CSVs in chunks')
        pbar = tqdm(total=len(files))
    for file in files:
        data = get_data(file)
        for j, row in enumerate(data):
            if file + str(j) not in processed_rows:
                venue_and_ids = re.search(name_and_ids, row['venue'])
                if venue_and_ids:
                    ids = venue_and_ids.group(2).split()
                    occurrences = sum(venues_occurrences[id]['occurrences'] for id in ids)
                    print(occurrences)
                    if occurrences <= chunk_size:
                        for id in ids:
                            for relevant_file in venues_occurrences[id]['files']:
                                even_chunk.extend([br for i, br in enumerate(get_data(relevant_file)) if i in venues_occurrences[id]['files'][relevant_file]])
                                processed_rows.update({relevant_file + str(i) for i in venues_occurrences[id]['files'][relevant_file]})
                        if len(even_chunk) >= chunk_size:
                            write_csv(os.path.join(output_dir, f'{counter}.csv'), even_chunk)
                            even_chunk = list()
                            counter += 1
                    else:
                        uneven_chunk.append(row)
                        processed_rows.add(file + str(j))
                        if len(uneven_chunk) >= chunk_size:
                            write_csv(os.path.join(output_dir, f'{counter}.csv'), uneven_chunk)
                            uneven_chunk = list()
                            counter += 1
                else:
                    uneven_chunk.append(row)
                    processed_rows.add(file + str(j))
                    if len(uneven_chunk) > chunk_size:
                        write_csv(os.path.join(output_dir, f'{counter}.csv'), uneven_chunk)
                        uneven_chunk = list()
                        counter += 1
        pbar.update() if verbose else None
    for chunk in [even_chunk, uneven_chunk]:
        if len(chunk):
            write_csv(os.path.join(output_dir, f'{counter}.csv'), chunk)
            counter += 1
    pbar.close() if verbose else None
