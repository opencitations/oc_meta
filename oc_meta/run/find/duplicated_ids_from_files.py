import argparse
import csv
import logging
import multiprocessing as mp
import os
import zipfile
from collections import defaultdict
from typing import Dict, Set

from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm

logging.basicConfig(filename='error_log_find_duplicated_ids_from_files.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_zip_file(zip_path: str) -> Dict[tuple, Set[str]]:
    entity_info = defaultdict(set)
    datacite_uses_identifier_scheme = URIRef("http://purl.org/spar/datacite/usesIdentifierScheme")
    literal_reification_has_literal_value = URIRef("http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for zip_file in zip_ref.namelist():
                try:
                    with zip_ref.open(zip_file) as rdf_file:
                        g = ConjunctiveGraph()
                        g.parse(data=rdf_file.read(), format="json-ld")
                        
                        for s, p, o in g.triples((None, datacite_uses_identifier_scheme, None)):
                            entity_id = str(s)
                            identifier_scheme = str(o)
                            literal_value = g.value(s, literal_reification_has_literal_value)
                            if identifier_scheme and literal_value:
                                key = (str(identifier_scheme), str(literal_value))
                                entity_info[key].add(entity_id)
                except Exception as e:
                    logging.error(f"Error processing file {zip_file} in {zip_path}: {str(e)}")
    except zipfile.BadZipFile:
        logging.error(f"Corrupted or invalid ZIP file: {zip_path}")
    except Exception as e:
        logging.error(f"Error opening ZIP file {zip_path}: {str(e)}")

    return entity_info

def read_and_analyze_zip_files(folder_path: str, csv_path: str):
    id_folder_path = os.path.join(folder_path, 'id')

    if not os.path.exists(id_folder_path):
        logging.error(f"The 'id' subfolder does not exist in path: {folder_path}")
        return

    zip_files = [os.path.join(root, file) for root, _, files in os.walk(id_folder_path) 
                 for file in files if file.endswith('.zip') and file != 'se.zip']

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_zip_file, zip_files), total=len(zip_files), desc="Analyzing ZIP files"))

    entity_info = defaultdict(set)
    for result in tqdm(results, desc="Merging results"):
        for key, value in result.items():
            entity_info[key].update(value)

    save_duplicates_to_csv(entity_info, csv_path)

def save_duplicates_to_csv(entity_info: Dict[tuple, Set[str]], csv_path: str):
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['surviving_entity', 'merged_entities'])

            for ids in tqdm(entity_info.values(), desc="Writing CSV"):
                if len(ids) > 1:
                    ids_list = list(ids)
                    csv_writer.writerow([ids_list[0], '; '.join(ids_list[1:])])
    except Exception as e:
        logging.error(f"Error saving CSV file {csv_path}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Find duplicate identifiers by reading RDF files inside ZIP archives in an 'id' subfolder.")
    parser.add_argument("folder_path", type=str, help="Path to the folder containing the 'id' subfolder")
    parser.add_argument("csv_path", type=str, help="Path to the CSV file to save duplicates")
    args = parser.parse_args()

    read_and_analyze_zip_files(args.folder_path, args.csv_path)

if __name__ == "__main__":
    main()