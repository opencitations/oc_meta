#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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
from meta.lib.file_manager import *
from meta.lib.master_of_regex import *
from meta.lib.csvmanager import CSVManager
from tqdm import tqdm


def run(csv_dir: str, output_dir: str, wanted_dois:str, verbose:bool=False) -> None:
    if verbose:
        print('[INFO:prepare_multiprocess] Getting the wanted DOIs')
    doi_set = CSVManager.load_csv_column_as_set(wanted_dois, 'doi') if wanted_dois else None
    files = os.listdir(csv_dir)
    if verbose:
        pbar = tqdm(total=len(files))
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    output_path = os.path.join(output_dir, 'relevant_venues.csv')
    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(csv_dir, file)
            relevant_venues_found = get_relevant_venues(file_path, doi_set)
            if relevant_venues_found:
                write_csv(path=output_path, datalist=relevant_venues_found, mode='a')
        if verbose:
            pbar.update()
    if verbose:
        pbar.close()
    
def split_by_publisher(file_path: str, output_dir: str) -> None:
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
        write_csv(path=output_file_path, datalist=data, mode='w')

def get_relevant_venues(file_path:str, doi_set:set=None) -> List[dict]:
    relevant_venues = list()
    data = get_data(file_path)
    for row in data:
        if row['venue']:
            if doi_set:
                if any(id.replace('doi:', '') in doi_set for id in row['id'].split()):
                    relevant_venues.append(generate_venue_row(row))
            else:
                relevant_venues.append(generate_venue_row(row))
    return relevant_venues

def generate_venue_row(row:dict) -> dict:
    venue_row = {k:'' for k,_ in row.items()}
    venue_name_and_ids = re.search(name_and_ids, row['venue'])
    venue_name = venue_name_and_ids.group(1) if venue_name_and_ids else row['venue']
    venue_ids = venue_name_and_ids.group(2) if venue_name_and_ids else ''
    venue_row['id'] = venue_ids
    venue_row['title'] = venue_name
    venue_row['type'] = 'journal'
    return venue_row