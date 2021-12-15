from meta.crossref.crossrefProcessing import *
import os
import csv
from argparse import ArgumentParser
from tqdm import tqdm

def preprocess(crossref_json_dir, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, verbose:bool=False) -> None:
    if verbose:
        log = 'Processing DOI-ORCID index'
        if wanted_doi_filepath:
            log += ' and wanted DOIs CSV'
        print(log)
    crossref_csv = crossrefProcessing(orcid_doi_filepath, wanted_doi_filepath)
    if verbose:
        pbar = tqdm(total=len(os.listdir(crossref_json_dir)))
    for filename in os.listdir(crossref_json_dir):
        if filename.endswith('.json') or filename.endswith('.json.gz'):
            json_file = os.path.join(crossref_json_dir, filename)
            new_filename = filename.replace('.gz', '').replace('.json', '.csv')
            filepath = os.path.join(csv_dir, new_filename)
            pathoo(filepath)
            data = crossref_csv.csv_creator(json_file)
            if data:
                with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
                    dict_writer = csv.DictWriter(output_file, data[0].keys(), delimiter=',', quotechar='"',
                                                quoting=csv.QUOTE_NONNUMERIC)
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
