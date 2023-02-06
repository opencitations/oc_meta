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


import os
from argparse import ArgumentParser

import yaml
from pebble import ProcessFuture, ProcessPool
from tqdm import tqdm

from oc_meta.lib.file_manager import pathoo
from oc_meta.plugins.csv_generator.csv_generator import (generate_csv,
                                                         process_archives,
                                                         process_br)


def task_done(task_output:ProcessFuture) -> None:
    PBAR.update()

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('csv_generator.py', description='This script generates output CSVs from the OpenCitations Meta RDF dump')
    arg_parser.add_argument('-c', '--c', dest='config', required=True, help='OpenCitations Meta configuration file location')
    arg_parser.add_argument('-o', '--output_dir', dest='output_dir', required=True,
                            help='The output directory where the CSV files will be stores')
    arg_parser.add_argument('-t', '--threshold', dest='threshold', required=True, default=5000, type=int,
                            help='How many lines the CSV output must contain')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int, help='Workers number')
    args = arg_parser.parse_args()
    with open(args.config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    output_dir = args.output_dir
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    resp_agent = settings['resp_agent']
    zip_output_rdf = settings['zip_output_rdf']
    if not os.path.exists(output_dir):
        pathoo(output_dir)
        process_archives(rdf_dir, 'br', output_dir, process_br, args.threshold)
    print('[csv_generator: INFO] Solving the OpenCitations Meta Identifiers recursively')
    filenames = os.listdir(output_dir)
    PBAR = tqdm(total=len(filenames))
    # for filename in filenames:
    #     generate_csv(filename, args.config, rdf_dir, dir_split_number, items_per_file, resp_agent, args.output_dir, zip_output_rdf)
    with ProcessPool(max_workers=args.max_workers, max_tasks=1) as executor:
        for filename in filenames:
            future:ProcessFuture = executor.schedule(
                function=generate_csv,
                args=(filename, args.config, rdf_dir, dir_split_number, items_per_file, resp_agent, args.output_dir, zip_output_rdf))
            future.add_done_callback(task_done)
        # PBAR.update()
    PBAR.close()