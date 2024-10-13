import argparse
import concurrent.futures
import csv
import os
from typing import List

from oc_meta.plugins.editor import MetaEditor
from oc_ocdm.graph import GraphSet
from rdflib import URIRef
from tqdm import tqdm


def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

def read_csv(csv_file) -> List[dict]:
    data = []
    with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if 'Done' not in row:
                row['Done'] = 'False'
            data.append(row)
    return data

def write_csv(csv_file, data):
    fieldnames = data[0].keys()
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def process_file(csv_file, meta_config, resp_agent, entity_types, stop_file_path):
    data = read_csv(csv_file)
    meta_editor = MetaEditor(meta_config, resp_agent, save_queries=True)

    # Creiamo un unico GraphSet per tutte le operazioni
    g_set = GraphSet(meta_editor.base_iri, custom_counter_handler=meta_editor.counter_handler)

    for row in data:
        if os.path.exists(stop_file_path):
            break

        entity_type = get_entity_type(row['surviving_entity'])
        if row.get('Done') != 'True' and entity_type in entity_types:
            surviving_entity = URIRef(row['surviving_entity'])
            merged_entities = row['merged_entities'].split('; ')

            for merged_entity in merged_entities:
                merged_entity = merged_entity.strip()
                try:
                    meta_editor.merge(g_set, surviving_entity, URIRef(merged_entity))
                except ValueError:
                    continue

            row['Done'] = 'True'

    # Salviamo le modifiche una sola volta alla fine
    meta_editor.save(g_set)

    write_csv(csv_file, data)
    return csv_file

def main():
    parser = argparse.ArgumentParser(description="Merge entities from CSV files in a folder.")
    parser.add_argument('csv_folder', type=str, help="Path to the folder containing CSV files")
    parser.add_argument('meta_config', type=str, help="Meta configuration string")
    parser.add_argument('resp_agent', type=str, help="Responsible agent string")
    parser.add_argument('--entity_types', nargs='+', default=['ra', 'br', 'id'], help="Types of entities to merge (ra, br, id)")
    parser.add_argument('--stop_file', type=str, default="stop.out", help="Path to the stop file")
    parser.add_argument('--workers', type=int, default=4, help="Number of parallel workers")
    args = parser.parse_args()

    if os.path.exists(args.stop_file):
        os.remove(args.stop_file)

    csv_files = [os.path.join(args.csv_folder, file) for file in os.listdir(args.csv_folder) if file.endswith('.csv')]

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_file, csv_file, args.meta_config, args.resp_agent, args.entity_types, args.stop_file): csv_file for csv_file in csv_files}

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Overall Progress"):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing file: {e}")


if __name__ == "__main__":
    main()