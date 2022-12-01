#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021 Simone Persiani <iosonopersia@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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


from __future__ import annotations

import os
import re
from argparse import ArgumentParser

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.lib.master_of_regex import name_and_ids


def map_metaid(ids: list, mapping: CSVManager):
    ids = set(ids)
    metaid = [identifier for identifier in ids if identifier.split(':')[0] == 'meta'][0]
    ids.remove(metaid)
    for other_id in ids:
        mapping.add_value(metaid, other_id)

def extract_metaid_mapping(input_dir:str, output_dir:str) -> None:
    mapping = CSVManager(output_path=output_dir)
    file_counter = 0
    threshold = 10000
    for filename in os.listdir(input_dir):
        if file_counter % threshold == 0:
            file_counter += 1
            mapping.dump_data(f'{file_counter}.csv')
        filepath = os.path.join(input_dir, filename)
        data = get_csv_data(filepath)
        for row in data:
            map_metaid(row['id'].split(), mapping)
            venue_name_and_ids = re.search(name_and_ids, row['venue'])
            if venue_name_and_ids:
                map_metaid(venue_name_and_ids.group(2).split(), mapping)
            publisher_name_and_ids = re.search(name_and_ids, row['publisher'])
            if publisher_name_and_ids:
                map_metaid(publisher_name_and_ids.group(2).split(), mapping)
            authors = row['author'].split('; ')
            editors = row['editor'].split('; ')
            for ra in [authors, editors]:
                for individual in ra:
                    individual_name_and_ids = re.search(name_and_ids, individual)
                    if individual_name_and_ids:
                        map_metaid(individual_name_and_ids.group(2).split(), mapping)
    if mapping.data:
        mapping.dump_data(f'{file_counter}.csv')


if __name__ == '__main__':
    arg_parser = ArgumentParser('Extract a mapping file between MetaIDs and other identifiers')
    arg_parser.add_argument('-i', '--input', dest='input', required=True,
                            help='The dirpath of the Meta output CSV files')
    arg_parser.add_argument('-o', '--output', dest='output', required=True,
                            help='The output dirpath')
    args = arg_parser.parse_args()
    extract_metaid_mapping(args.input, args.output)