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


import os, re, csv
from typing import List, Tuple, Set, Dict, Union
from meta.lib.master_of_regex import *
from tqdm import tqdm
from pymongo import MongoClient

csv.field_size_limit(1000000)

class MongoClass:
    def __init__(self, host: str = 'localhost', port: int = 27017):
        client = MongoClient(host, port)
        db = client.meta
        self.collection = db.publishers
    
    def insert_data(self, data: Union[dict, list]) -> None:
        for item in data:
            self.collection.update_one({'_id': item['_id']}, {
                '$push': {'rows': item for item in item['rows']}, 
                '$set': {'label': item['label']}}, 
                upsert=True)               


def cycle_on_csvs(csv_dir: str, func: callable):
    pbar = tqdm(total=len(os.listdir(csv_dir)))
    output = dict()
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(csv_dir, filename)
            data_initial = open(filepath, 'r', encoding='utf8')
            data = csv.DictReader((line.replace('\0','') for line in data_initial), delimiter=',')
            func(data)
        pbar.update()
    pbar.close()
    return output
                
def are_there_multi_pub_venues(data: List[dict]) -> Set[str]:
    delimiter_in_field = set()
    delimiter = ' ;and; '
    for row in data:
        for field in {row['id'], row['author'], row['venue'], row['editor'], row['publisher']}:
            if delimiter in field:
                delimiter_in_field.add(row['id'])
    return delimiter_in_field

def split_by_publisher(csv_dir: str) -> Tuple[dict]:
    mongo = MongoClass()
    pbar = tqdm(total=len(os.listdir(csv_dir)))
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(csv_dir, filename)
            data_initial = open(filepath, 'r', encoding='utf8')
            data = csv.DictReader((line.replace('\0','') for line in data_initial), delimiter=',')
            documents : List[Dict[str, List[dict]]] = list()
            for row in data:
                if row['publisher']:
                    publisher_id = re.search(ids_inside_square_brackets, row['publisher'])
                    publisher_id = publisher_id.group(1) if publisher_id else row['publisher']
                    i_to_update = next((i for i, v in enumerate(documents) if v['_id'] == publisher_id), None)
                    if not i_to_update:
                        new_document = {'_id': publisher_id, 'label': row['publisher'], 'rows': [row]}
                        documents.append(new_document)
                    else:
                        documents[i_to_update]['rows'].append(row)
            mongo.insert_data(documents)
        pbar.update()
    pbar.close()

def dump_csvs_by_publisher(data: Dict[str, List[dict]], output_dir: str):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    for k, v in data.items():
        filename = k.replace('crossref:', '')
        filename = os.path.join(output_dir, f'{filename}.csv')
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            dict_writer = csv.DictWriter(f, v[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
            dict_writer.writeheader()
            dict_writer.writerows(v)