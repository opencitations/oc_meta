import csv
import os
import sys
from argparse import ArgumentParser
from tarfile import TarInfo

import yaml
from tqdm import tqdm

from oc_meta.lib.file_manager import normalize_path
from oc_meta.lib.jsonmanager import *
from oc_meta.plugins.datacite.datacite_processing import *
from oc_meta.preprocessing.datacite import *


def preprocess(datacite_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False) -> None:
    if verbose:
        if publishers_filepath or orcid_doi_filepath or wanted_doi_filepath:
            what = list()
            if publishers_filepath:
                what.append('publishers mapping')
            if orcid_doi_filepath:
                what.append('DOI-ORCID index')
            if wanted_doi_filepath:
                what.append('wanted DOIs CSV')
            log = '[INFO: datacite_process] Processing: ' + '; '.join(what)
            print(log)

    print("Processing Phase: started")
    datacite_csv = DataciteProcessing(orcid_index=orcid_doi_filepath, doi_csv=wanted_doi_filepath, publishers_filepath=publishers_filepath)
    if verbose:
        print(f'[INFO: datacite_process] Getting all files from {datacite_json_dir}')
    all_files, targz_fd = get_all_files(datacite_json_dir, cache)
    if verbose:
        pbar = tqdm(total=len(all_files))
    for filename in all_files:
        source_data = load_json(filename, targz_fd)
        filename = filename.name if isinstance(filename, TarInfo) else filename
        filename_without_ext = filename.replace('.json', '').replace('.tar', '').replace('.gz', '')
        filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
        pathoo(filepath)
        data = list()
        for item in source_data['data']:
            tabular_data = datacite_csv.csv_creator(item)
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
        if verbose:
            pbar.update() if verbose else None
    if cache:
        if os.path.exists(cache):
            os.remove(cache)
    pbar.close() if verbose else None

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

if __name__ == '__main__':
    arg_parser = ArgumentParser('datacite_process.py', description='This script creates CSV files from DATACITE JSON zipped JSON, enriching data through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-dc', '--datacite', dest='datacite_json_dir', required=required,
                            help='DataCite json files directory')
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
    arg_parser.add_argument('-spp', '--skippreprocess', dest='skippreprocess', required=False,
                            help='Specify -spp True if the input material was already preprocessed, i.e. if the '
                                 ' entities not involved in citations were already removed from the dump')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    datacite_json_dir = settings['datacite_json_dir'] if settings else args.datacite_json_dir
    datacite_json_dir = normalize_path(datacite_json_dir)
    preprocessed_files_dir = os.path.join(datacite_json_dir, "preprocessed_json")
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
    skip_preprocess = settings['no_preprocess_needed'] if settings else args.skippreprocess
    verbose = settings['verbose'] if settings else args.verbose
    if not skip_preprocess:
        print("Input Files Preprocessing Phase: started")
        DatacitePreProcessing(datacite_json_dir, preprocessed_files_dir, 100000)
    print("Data Preprocessing Phase: started")
    preprocess(datacite_json_dir=preprocessed_files_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose)
