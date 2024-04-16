import argparse
import zipfile
import os
import json
from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

def process_json_content(json_content):
    entity_count = {}
    total_entities = {}
    for item in json_content:
        graph = item["@graph"]
        for entity in graph:
            entity_id = entity["@id"]
            entity_type = get_entity_type(entity_id)
            if entity_type:
                if entity_type in total_entities:
                    total_entities[entity_type] += 1
                else:
                    total_entities[entity_type] = 1

                if "http://www.w3.org/ns/prov#hadPrimarySource" not in entity:
                    if entity_type in entity_count:
                        entity_count[entity_type] += 1
                    else:
                        entity_count[entity_type] = 1

    return entity_count, total_entities

def process_zip_file(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        file_info = z.infolist()[0]
        if file_info.filename.endswith('.json'):
            with z.open(file_info.filename) as file:
                json_content = json.load(file)
                entity_count, total_count = process_json_content(json_content)
                return entity_count, total_count
    return {}, {}

def combine_counts(main_count, new_count):
    for key, value in new_count.items():
        if key in main_count:
            main_count[key] += value
        else:
            main_count[key] = value
    return main_count

def find_and_process_zip_files(folder_path):
    zip_files = [os.path.join(root, file)
                 for root, _, files in os.walk(folder_path)
                 for file in files if file == 'se.zip']
    pool = Pool(cpu_count())
    results = list(tqdm(pool.imap(process_zip_file, zip_files), total=len(zip_files)))
    pool.close()
    pool.join()

    final_count = {}
    total_count = {}
    for entity_count, total_entities in results:
        final_count = combine_counts(final_count, entity_count)
        total_count = combine_counts(total_count, total_entities)

    with open("missing_primary_source_log.txt", "w") as log_file:
        for entity_type, missing_count in final_count.items():
            total = total_count.get(entity_type, 0)
            percentage = (missing_count / total * 100) if total > 0 else 0
            log_entry = f"Entity Type: {entity_type}, Missing 'hadPrimarySource': {missing_count}/{total} ({percentage:.2f}%)"
            print(log_entry)
            log_file.write(log_entry + "\n")

def main():
    parser = argparse.ArgumentParser(description="Explore and process se.zip files for specific RDF data.")
    parser.add_argument("folder_path", type=str, help="Root folder to search for se.zip files.")
    args = parser.parse_args()
    find_and_process_zip_files(args.folder_path)

if __name__ == "__main__":
    main()
