from meta.plugins.crossref.crossref_processing import *
from meta.lib.jsonmanager import *
import os
import csv
import sys
import yaml
from argparse import ArgumentParser
from tqdm import tqdm

def preprocess(crossref_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, verbose:bool=False) -> None:
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
    all_files, targz_fd = get_all_files(crossref_json_dir)
    if verbose:
        pbar = tqdm(total=len(all_files))
    for idx, file in enumerate(all_files):
        data = load_json(file, targz_fd)
        new_filename = f'{idx}.csv'
        filepath = os.path.join(csv_dir, new_filename)
        pathoo(filepath)
        data = crossref_csv.csv_creator(data)
        if data:
            with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, data[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                dict_writer.writeheader()
                dict_writer.writerows(data)
        if verbose:
            pbar.update(1)
    if verbose:
        pbar.close()

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

if __name__ == '__main__':
    arg_parser = ArgumentParser('crossref_process.py', description='This script creates CSV files from Crossref JSON files, enriching them through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = all(arg in sys.argv for arg in {'--config', '-c'})
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
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as file:
            settings = yaml.full_load(file)
    crossref_json_dir = settings['crossref_json_dir'] if settings else args.crossref_json_dir
    csv_dir = settings['output'] if settings else args.csv_dir
    publishers_filepath = settings['publishers_filepath'] if settings else args.publishers_filepath
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    wanted_doi_filepath = settings['wanted_doi_filepath'] if settings else args.wanted_doi_filepath
    verbose = settings['verbose'] if settings else args.verbose
    preprocess(crossref_json_dir=crossref_json_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, verbose=verbose)
