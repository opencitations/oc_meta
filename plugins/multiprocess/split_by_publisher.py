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
from typing import List, Dict
from meta.lib.master_of_regex import *
from tqdm import tqdm

csv.field_size_limit(1000000)

def run(csv_dir: str, output_dir: str) -> None:
    files = os.listdir(csv_dir)
    pbar = tqdm(total=len(files))
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    for file in files:
        if file.endswith(".csv"):
            split_by_publisher(os.path.join(csv_dir, file), output_dir)
        pbar.update()
    pbar.close()

def split_by_publisher(file_path: str, output_dir: str) -> None:
    data_initial = open(file_path, 'r', encoding='utf8')
    valid_data = (line.replace('\0','') for line in data_initial)
    data = csv.DictReader(valid_data, delimiter=',')
    data_by_publisher:Dict[str, List] = dict()
    for row in data:
        if row['publisher']:
            id = re.search(ids_inside_square_brackets, row['publisher'])
            publisher = id.group(1) if id else row['publisher']
            data_by_publisher.setdefault(publisher, list()).append(row)
    for publisher, data in data_by_publisher.items():
        dump_csv_by_publisher(publisher, data, output_dir)

def dump_csv_by_publisher(publisher: str, data: List[dict], output_path: str) -> None:
    publisher = publisher.replace(':', '_')
    publisher += '.csv'
    output_file_path = os.path.join(output_path, publisher)
    with open(output_file_path, 'a', encoding='utf8', newline='') as output_file:
        writer = csv.DictWriter(output_file, data[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        writer.writeheader()
        writer.writerows(data)