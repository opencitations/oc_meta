from meta.plugins.crossref.crossrefProcessing import *
from meta.lib.jsonmanager import *
import os
import csv
import json
from argparse import ArgumentParser
from tqdm import tqdm

def preprocess(crossref_json_dir:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, verbose:bool=False) -> None:
    if verbose:
        log = '[INFO: crossref_process] Processing DOI-ORCID index'
        if wanted_doi_filepath:
            log += ' and wanted DOIs CSV'
        print(log)
    crossref_csv = crossrefProcessing(None, wanted_doi_filepath)
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
    arg_parser = ArgumentParser('run_preprocess.py', description='This script create csv files from Crossref json,'
                                                                 ' enriching them thanks to an doi-orcid index')

    arg_parser.add_argument('-c', '--crossref', dest='crossref_json_dir', required=True,
                            help='Crossref json files directory')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=True,
                            help='Orcid-doi index filepath, to enrich data')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=True,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')

    args = arg_parser.parse_args()

    preprocess(args.crossref_json_dir, args.orcid_doi_filepath, args.csv_dir, args.wanted_doi_filepath, args.verbose)
