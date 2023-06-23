#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2023 Arcangelo Massari <arcangelo.massari@unibo.it>
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


import os
from argparse import ArgumentParser

from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data, write_csv

if __name__ == '__main__':
    arg_parser = ArgumentParser('remove_duplicated_ids.py', description='This script removes rows with duplicated ids from the Meta input CSVs')
    arg_parser.add_argument('-c', '--csv', dest='csv_dir', required=True,
                            help='The path to the directory containing the CSV files')
    arg_parser.add_argument('-o', '--output', dest='output_dir', required=True,
                            help='The path to the directory that will contain the CSVs in output')
    args = arg_parser.parse_args()
    data_found = set()
    ids_found = set()
    fieldnames = ['id', 'title', 'author', 'issue', 'volume', 'venue', 'pub_date', 'page', 'type', 'editor', 'publisher']
    pbar = tqdm(total=len(os.listdir(args.csv_dir)))
    for filename in os.listdir(args.csv_dir):
        data = get_csv_data(os.path.join(args.csv_dir, filename))
        output_data = []
        for row in data:
            row['id'] = ' '.join(sorted(row['id'].split()))
            row['author'] = '; '.join(sorted(row['author'].split('; ')))
            row['editor'] = '; '.join(sorted(row['editor'].split('; ')))
            data_in_row = ''.join([row[field] for field in fieldnames])
            if data_in_row not in data_found:
                output_data.append(row)
            data_found.add(data_in_row)
        write_csv(os.path.join(args.output_dir, filename), output_data, fieldnames)
        pbar.update()
    pbar.close()