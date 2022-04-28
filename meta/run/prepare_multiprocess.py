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
# SOFTWARE


from argparse import ArgumentParser
from meta.lib.file_manager import normalize_path
from meta.run.meta_process import MetaProcess, run_meta_process
from meta.plugins.multiprocess.prepare_multiprocess import prepare_relevant_items, split_csvs_in_chunks
import os
import shutil
import yaml


if __name__ == '__main__':
    arg_parser = ArgumentParser('prepare_multiprocess.py', description='Venues, authors and editors are preprocessed not to create duplicates when running Meta in multi-process')
    arg_parser.add_argument('-c', '--config', dest='config', required=True, help='Configuration file path')
    args = arg_parser.parse_args()
    config = args.config
    with open(config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    csv_dir = normalize_path(settings['input_csv_dir'])
    items_per_file = settings['items_per_file']
    workers_numbers = settings['workers_number']
    verbose = settings['verbose']
    meta_process = MetaProcess(config)
    TMP_DIR = os.path.join(meta_process.base_output_dir, 'tmp')
    if not all(os.path.exists(os.path.join(TMP_DIR, directory)) for directory in ['venues', 'ids', 'publishers', 'people']):
        split_csvs_in_chunks(csv_dir=csv_dir, output_dir=TMP_DIR, chunk_size=1000, verbose=verbose)
        os.rename(csv_dir, csv_dir + '_old')
        os.mkdir(csv_dir)
        for file in os.listdir(TMP_DIR):
            shutil.move(os.path.join(TMP_DIR, file), csv_dir)
        prepare_relevant_items(csv_dir=csv_dir, output_dir=TMP_DIR, items_per_file=items_per_file, verbose=verbose)
    venues_dir = os.path.join(TMP_DIR, 'venues')
    meta_process.input_csv_dir = venues_dir
    run_meta_process(meta_process=meta_process)
    ids_dir = os.path.join(TMP_DIR, 'ids')
    meta_process.input_csv_dir = ids_dir
    meta_process.workers_number = workers_numbers
    run_meta_process(meta_process=meta_process)
    publishers_dir = os.path.join(TMP_DIR, 'publishers')
    meta_process.input_csv_dir = publishers_dir
    run_meta_process(meta_process=meta_process, resp_agents_only=True)
    people_dir = os.path.join(TMP_DIR, 'people')
    meta_process.input_csv_dir = people_dir
    run_meta_process(meta_process=meta_process, resp_agents_only=True)
    shutil.rmtree(TMP_DIR)