import argparse
import csv
import logging
import multiprocessing as mp
import os
import shutil
import tempfile
import zipfile
from collections import defaultdict
from typing import Dict, List, Set

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
                        
                        for s, _, o in g.triples((None, datacite_uses_identifier_scheme, None)):
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

def save_chunk_to_temp_csv(entity_info: Dict[tuple, Set[str]], temp_file_path: str):
    with open(temp_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['identifier_scheme', 'literal_value', 'entity_ids'])
        for (scheme, value), ids in entity_info.items():
            csv_writer.writerow([scheme, value, ';'.join(ids)])

def load_and_merge_temp_csv(temp_file_path: str, entity_info: Dict[tuple, Set[str]]):
    with open(temp_file_path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            key = (row['identifier_scheme'], row['literal_value'])
            ids = set(row['entity_ids'].split(';'))
            entity_info[key].update(ids)

def process_chunk(zip_files_chunk: List[str], temp_dir: str, chunk_index: int) -> str:
    entity_info = defaultdict(set)

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(process_zip_file, zip_files_chunk)

    for result in results:
        for key, value in result.items():
            entity_info[key].update(value)

    temp_file_path = os.path.join(temp_dir, f'chunk_{chunk_index}.csv')
    save_chunk_to_temp_csv(entity_info, temp_file_path)

    return temp_file_path

def read_and_analyze_zip_files(folder_path: str, csv_path: str, chunk_size: int = 5000, temp_dir: str = None):
    id_folder_path = os.path.join(folder_path, 'id')

    if not os.path.exists(id_folder_path):
        logging.error(f"The 'id' subfolder does not exist in path: {folder_path}")
        return

    zip_files = [os.path.join(root, file) for root, _, files in os.walk(id_folder_path)
                 for file in files if file.endswith('.zip') and file != 'se.zip']

    if temp_dir is None:
        temp_dir = tempfile.mkdtemp(prefix='oc_meta_duplicates_')
    else:
        os.makedirs(temp_dir, exist_ok=True)

    try:
        chunks = [zip_files[i:i + chunk_size] for i in range(0, len(zip_files), chunk_size)]
        temp_files = []

        print(f"Processing {len(zip_files)} ZIP files in {len(chunks)} chunks of max {chunk_size} files each")
        print(f"Temporary files will be stored in: {temp_dir}")

        for chunk_index, chunk in enumerate(tqdm(chunks, desc="Processing chunks")):
            temp_file = process_chunk(chunk, temp_dir, chunk_index)
            temp_files.append(temp_file)

        print("Merging chunk results...")
        entity_info = defaultdict(set)
        for temp_file in tqdm(temp_files, desc="Merging chunks"):
            load_and_merge_temp_csv(temp_file, entity_info)

        save_duplicates_to_csv(entity_info, csv_path)

    finally:
        if temp_dir.startswith(tempfile.gettempdir()):
            shutil.rmtree(temp_dir, ignore_errors=True)

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
    parser.add_argument("--chunk-size", type=int, default=5000,
                        help="Number of ZIP files to process per chunk (default: 5000)")
    parser.add_argument("--temp-dir", type=str, default=None,
                        help="Directory for temporary files (default: system temp directory)")
    args = parser.parse_args()

    read_and_analyze_zip_files(args.folder_path, args.csv_path, args.chunk_size, args.temp_dir)

if __name__ == "__main__":
    main()