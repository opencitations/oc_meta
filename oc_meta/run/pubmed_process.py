import sys
import yaml
from oc_meta.plugins.pubmed.pubmed_processing import *
import os.path
from os.path import exists
import csv
import pandas as pd
from argparse import ArgumentParser
from datetime import datetime
from tqdm import tqdm
from oc_meta.lib.file_manager import normalize_path
from oc_meta.lib.jsonmanager import get_all_files_by_type


def to_meta_file(cur_n, lines, interval, csv_dir):

    if int(cur_n) != 0 and int(cur_n) % int(interval) == 0:
        # to be logged: print("Processed lines:", cur_n, ". Reduced csv nr.", cur_n // self._interval)
        filename = "CSVFile_" + str(cur_n // interval)
        filepath = os.path.join(csv_dir, f'{os.path.basename(filename)}.csv')
        if exists(os.path.join(filepath)):
            cur_datetime = datetime.now()
            dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
            filepath = filepath[:-len(".csv")] + "_" + dt_string + ".csv"

        with open(filepath, "w", encoding="utf8", newline="") as f_out:
            dict_writer = csv.DictWriter(f_out, lines[0].keys(), delimiter=',', quotechar='"',
                                         quoting=csv.QUOTE_ALL, escapechar='\\')
            dict_writer.writeheader()
            dict_writer.writerows(lines)

        lines = []
        return lines
    else:
        return lines


def preprocess(pubmed_csv_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, journals_filepath:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False, interval = 1000, testing=True) -> None:
    if not interval:
        interval = 1000
    else:
        try:
            interval = int(interval)
        except:
            interval = 1000

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    filter = ["pmid", "doi", "title", "authors", "year", "journal", "references"]
    if verbose:
        if publishers_filepath or orcid_doi_filepath or wanted_doi_filepath:
            what = list()
            if publishers_filepath:
                what.append('publishers mapping')
            if orcid_doi_filepath:
                what.append('DOI-ORCID index')
            if wanted_doi_filepath:
                what.append('wanted DOIs CSV')
            log = '[INFO: pubmed_process] Processing: ' + '; '.join(what)
            print(log)

    print("Processing Phase: started")
    pubmed_csv = PubmedProcessing(orcid_index=orcid_doi_filepath, doi_csv=wanted_doi_filepath, publishers_filepath_pubmed=publishers_filepath, journals_filepath=journals_filepath, testing=testing)
    if verbose:
        print(f'[INFO: pubmed_process] Getting all files from {pubmed_csv_dir}')


    all_files, targz_fd = get_all_files_by_type(pubmed_csv_dir, ".csv", cache)
    count = 0
    lines = []

    for file_idx, file in enumerate(tqdm(all_files), 1):
        chunksize = 100000
        with pd.read_csv(file,  usecols=filter, chunksize=chunksize) as reader:
            for chunk in reader:
                chunk.fillna("", inplace=True)
                df_dict_list = chunk.to_dict("records")
                filt_values = [d for d in df_dict_list if (d.get("cited_by") or d.get("references"))]

                for item in filt_values:
                    tabular_data = pubmed_csv.csv_creator(item)

                    if tabular_data:
                        lines.append(tabular_data)
                        count += 1
                        if int(count) != 0 and int(count) % int(interval) == 0:
                            last_processed = lines[-1].get("id")
                            lines = to_meta_file(count, lines, interval, csv_dir)
                            pubmed_csv.save_updated_pref_publishers_map()

                            if cache:
                                with open(cache, 'a', encoding='utf-8') as aux_file:
                                    aux_file.write(os.path.basename(last_processed) + '\n')

    if len(lines) > 0:
        count = count + (interval - (int(count) % int(interval)))
        to_meta_file(count, lines, interval, csv_dir)
        pubmed_csv.save_updated_pref_publishers_map()

    if cache:
        if os.path.exists(cache):
            os.remove(cache)


def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

if __name__ == '__main__':
    arg_parser = ArgumentParser('pubmed_process.py', description='This script creates meta CSV files from pubmed preprocessed dump, enriching data through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-dc', '--pubmed', dest='pubmed_csv_dir', required=required,
                            help='pubmed preprocessed csv files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-p', '--publishers', dest='publishers_filepath', required=False,
                            help='CSV file path containing information about publishers (id, name, prefix, from)')
    arg_parser.add_argument('-j', '--journals', dest='journals_filepath', required=False,
                            help='JSON filepath containing information about the ISSN - journal names mapping')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=False,
                            help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                        help='The cache file path. This file will be deleted at the end of the process')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    arg_parser.add_argument('-int', '--interval', dest='interval',type=bool, required=False, default=1000,
                            help='int number of lines for each output csv. If nothing is declared, the default is 1000')
    arg_parser.add_argument('-t', '--testing', dest='testing',type=bool, required=False, default=True,
                            help='testing flag to define what to use for data validation (fakeredis instance or real redis DB)')
    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    pubmed_csv_dir = settings['pubmed_csv_dir'] if settings else args.pubmed_csv_dir
    interval = settings['interval'] if settings else args.interval
    pubmed_csv_dir = normalize_path(pubmed_csv_dir)
    preprocessed_files_dir = os.path.join(pubmed_csv_dir, "preprocessed_csv")
    csv_dir = settings['output'] if settings else args.csv_dir
    csv_dir = normalize_path(csv_dir)
    publishers_filepath = settings['publishers_filepath'] if settings else args.publishers_filepath
    publishers_filepath = normalize_path(publishers_filepath) if publishers_filepath else None
    journals_filepath = settings['journals_filepath'] if settings else args.journals_filepath
    journals_filepath = normalize_path(journals_filepath) if journals_filepath else None
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    orcid_doi_filepath = normalize_path(orcid_doi_filepath) if orcid_doi_filepath else None
    wanted_doi_filepath = settings['wanted_doi_filepath'] if settings else args.wanted_doi_filepath
    wanted_doi_filepath = normalize_path(wanted_doi_filepath) if wanted_doi_filepath else None
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    verbose = settings['verbose'] if settings else args.verbose
    testing = settings['testing'] if settings else args.testing
    print("Data Preprocessing Phase: started")
    preprocess(pubmed_csv_dir=preprocessed_files_dir, publishers_filepath=publishers_filepath, journals_filepath=journals_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose, interval=interval, testing=testing)
