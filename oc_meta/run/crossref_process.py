#!/usr/bin/python
# -*- coding: utf-8 -*-
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


from argparse import ArgumentParser
from oc_meta.lib.file_manager import normalize_path
from oc_meta.lib.jsonmanager import *
from oc_meta.plugins.crossref.crossref_processing import *
from tarfile import TarInfo
from tqdm import tqdm
import csv
import yaml
import os
import sys


def preprocess(crossref_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False) -> None:
    if verbose:
        if publishers_filepath or orcid_doi_filepath or wanted_doi_filepath:
            what = list()
            if publishers_filepath:
                what.append('publishers mapping')
            if orcid_doi_filepath:
                what.append('DOI-ORCID index')
            if wanted_doi_filepath:
                what.append('wanted DOIs CSV')
            log = '[INFO: crossref_process] Processing: ' + '; '.join(what)
            print(log)
    crossref_csv = CrossrefProcessing(orcid_index=orcid_doi_filepath, doi_csv=wanted_doi_filepath, publishers_filepath=publishers_filepath)
    if verbose:
        print(f'[INFO: crossref_process] Getting all files from {crossref_json_dir}')
    all_files, targz_fd = get_all_files(crossref_json_dir, cache)
    if verbose:
        pbar = tqdm(total=len(all_files))
    for filename in all_files:
        source_data = load_json(filename, targz_fd)
        filename = filename.name if isinstance(filename, TarInfo) else filename
        filename_without_ext = filename.replace('.json', '').replace('.tar', '').replace('.gz', '')
        filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
        pathoo(filepath)
        data = list()
        for item in source_data['items']:
            tabular_data = crossref_csv.csv_creator(item)
            if tabular_data:
                data.append(tabular_data)
        if data:
            with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, data[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                dict_writer.writeheader()
                dict_writer.writerows(data)
        if cache:
            with open(cache, 'a', encoding='utf-8') as aux_file:
                aux_file.write(os.path.basename(filename) + '\n')
        pbar.update() if verbose else None
    if cache:
        if os.path.exists(cache):
            os.remove(cache)
    pbar.close() if verbose else None

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

if __name__ == '__main__':
    arg_parser = ArgumentParser('crossref_process.py', description='This script creates CSV files from Crossref JSON files, enriching them through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-cf', '--crossref', dest='crossref_json_dir', required=required,
                            help='Crossref json files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-p', '--publishers', dest='publishers_filepath', required=False,
                            help='CSV file path containing information about publishers (id, name, prefix)')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=False,
                            help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                        help='The cache file path. This file will be deleted at the end of the process')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    crossref_json_dir = settings['crossref_json_dir'] if settings else args.crossref_json_dir
    crossref_json_dir = normalize_path(crossref_json_dir)
    csv_dir = settings['output'] if settings else args.csv_dir
    csv_dir = normalize_path(csv_dir)
    publishers_filepath = settings['publishers_filepath'] if settings else args.publishers_filepath
    publishers_filepath = normalize_path(publishers_filepath) if publishers_filepath else None
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    orcid_doi_filepath = normalize_path(orcid_doi_filepath) if orcid_doi_filepath else None
    wanted_doi_filepath = settings['wanted_doi_filepath'] if settings else args.wanted_doi_filepath
    wanted_doi_filepath = normalize_path(wanted_doi_filepath) if wanted_doi_filepath else None
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    verbose = settings['verbose'] if settings else args.verbose
    preprocess(crossref_json_dir=crossref_json_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose)
