import csv
import os
import sys
from argparse import ArgumentParser
from tarfile import TarInfo
import yaml
from tqdm import tqdm
from oc_meta.lib.file_manager import normalize_path
from oc_meta.lib.jsonmanager import *
from oc_meta.plugins.jalc.jalc_processing import JalcProcessing
import ndjson
import re

def preprocess(jalc_json_dir:str, citing_entities_filepath:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False) -> None:
    if verbose:
        if citing_entities_filepath or orcid_doi_filepath or wanted_doi_filepath or publishers_filepath:
            what = list()
            if publishers_filepath:
                what.append('publishers mapping')
            if orcid_doi_filepath:
                what.append('DOI-ORCID index')
            if wanted_doi_filepath:
                what.append('wanted DOIs CSV')
            if citing_entities_filepath:
                what.append('citing entities index')
            log = '[INFO: datacite_process] Processing: ' + '; '.join(what)
            print(log)

    print("Processing Phase: started")
    jalc_csv = JalcProcessing(citing_entities= citing_entities_filepath, orcid_index=orcid_doi_filepath, doi_csv=wanted_doi_filepath, publishers_filepath=publishers_filepath)
    if verbose:
        print(f'[INFO: datacite_process] Getting all files from {jalc_json_dir}')
    all_files, targz_fd = get_jalc_ndjson(jalc_json_dir)
    if verbose:
        pbar = tqdm(total=len(all_files))
    for filename in all_files:
        print(filename)
        source_data = load_ndjson(filename)
        filename = filename.name if isinstance(filename, TarInfo) else filename
        filename_without_ext = filename.replace('.ndjson', '').replace('.tar', '').replace('.gz', '')
        filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
        pathoo(filepath)
        data = list()
        for item in source_data:
            tabular_data = jalc_csv.csv_creator(item)
            if tabular_data:
                data.append(tabular_data)
            for citation_dic in item['citation_list']:
                citation_id = citation_dic['doi']
                if citing_entities_filepath:
                    if citation_id.startswith("doi:"):
                        doi = re.sub('^doi:', '', citation_id)
                    if doi not in jalc_csv.citing_entities_set:
                        tabular_data_cited = jalc_csv.csv_creator(citation_dic)
                        if tabular_data_cited:
                            data.append(tabular_data_cited)
                else:
                    tabular_data_cited = jalc_csv.csv_creator(citation_dic)
                    if tabular_data_cited:
                        data.append(tabular_data_cited)
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


def get_jalc_ndjson(jalc_ndjson_dir) -> list:
    result= []
    targz_fd = None
    if os.path.isdir(jalc_ndjson_dir):
        for cur_dir, _, cur_files in os.walk(jalc_ndjson_dir):
          for cur_file in cur_files:
            if cur_file.endswith(".ndjson"):
                result.append(jalc_ndjson_dir + sep + cur_file)
    else:
        print("It is not possible to process the input path.")
    return result, targz_fd


def load_ndjson(file):
    result = None
    if file.endswith(".ndjson"):  # type: ignore
        with open(file, encoding="utf8") as f: # type: ignore
            result = ndjson.load(f)
    return result


def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


if __name__ == '__main__':
    arg_parser = ArgumentParser('jalc_process.py', description='This script creates CSV files from JALC NDJSON, enriching data through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-ja', '--jalc', dest='jalc_json_dir', required=required,
                            help='Jalc ndjson files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-cit', '--citing', dest='citing_entities_filepath', required=False,
                            help='file path produced in the preprocessing phase containing the DOI of all the citing entities')
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
    jalc_json_dir = settings['jalc_json_dir'] if settings else args.jalc_json_dir
    jalc_json_dir = normalize_path(jalc_json_dir)
    csv_dir = settings['output'] if settings else args.csv_dir
    csv_dir = normalize_path(csv_dir)
    citing_entities_filepath = settings['citing_entities_filepath'] if settings else args.citing_entities_filepath
    citing_entities_filepath = normalize_path(citing_entities_filepath) if citing_entities_filepath else None
    publishers_filepath = settings['publishers_filepath'] if settings else args.publishers_filepath
    publishers_filepath = normalize_path(publishers_filepath) if publishers_filepath else None
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    orcid_doi_filepath = normalize_path(orcid_doi_filepath) if orcid_doi_filepath else None
    wanted_doi_filepath = settings['wanted_doi_filepath'] if settings else args.wanted_doi_filepath
    wanted_doi_filepath = normalize_path(wanted_doi_filepath) if wanted_doi_filepath else None
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    verbose = settings['verbose'] if settings else args.verbose
    print("Jalc Preprocessing Phase: started")
    preprocess(jalc_json_dir=jalc_json_dir, citing_entities_filepath=citing_entities_filepath, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose)
