import argparse
import csv
from oc_meta.plugins.editor import MetaEditor
from rdflib import URIRef
from tqdm import tqdm
import multiprocessing
from multiprocessing import Pool, Manager, Queue
from SPARQLWrapper import SPARQLWrapper, JSON
import time 
import tempfile
import shutil
import os

BR_PRIORITY_GROUPS = [
    ["http://purl.org/spar/fabio/Series", "http://purl.org/spar/fabio/BookSeries", 
     "http://purl.org/spar/fabio/Journal"],
    ["http://purl.org/spar/fabio/ReferenceBook", "http://purl.org/spar/fabio/JournalVolume", 
     "http://purl.org/spar/fabio/AcademicProceedings", "http://purl.org/spar/fabio/Book"],
    ["http://purl.org/spar/fabio/JournalIssue"]
]

def get_entity_type_sparql(entity_url, triplestore_endpoint):
    query = f"""
    SELECT ?type 
    WHERE {{
        <{entity_url}> a ?type .
        FILTER (?type != <http://purl.org/spar/fabio/Expression>)
    }}"""
    sparql = SPARQLWrapper(triplestore_endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    if results["results"]["bindings"]:
        return results["results"]["bindings"][0]["type"]["value"]
    else:
        return None

def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

def classify_br_entities(data, triplestore_endpoint):
    classified_data = {i: [] for i in range(len(BR_PRIORITY_GROUPS) + 1)}

    for row in tqdm(data, desc="Classifying entities", unit="entity"):
        entity_type = get_entity_type(row['surviving_entity'])
        if entity_type == 'br':
            specific_type = get_entity_type_sparql(row['surviving_entity'], triplestore_endpoint)
            for i, group in enumerate(BR_PRIORITY_GROUPS):
                if specific_type in group:
                    classified_data[i].append(row)
                    break
            else:
                classified_data[len(BR_PRIORITY_GROUPS)].append(row)
        else:
            classified_data[len(BR_PRIORITY_GROUPS)].append(row)

    return classified_data

def read_csv(csv_file):
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

def validate_entity(entity_url, triplestore_endpoint):
    """
    Validate if the entity exists and is a valid OCDM entity in the triplestore.
    :param entity_url: The URL of the entity to validate.
    :param triplestore_endpoint: SPARQL endpoint of the triplestore.
    :return: True if valid, False otherwise.
    """
    query = f"""
        ASK WHERE {{
            <{entity_url}> ?p ?o .
        }}"""
    sparql = SPARQLWrapper(triplestore_endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results['boolean']

def merge_entities(csv_file, meta_config, resp_agent, entity_types, triplestore_endpoint):
    meta_editor = MetaEditor(meta_config, resp_agent)
    data = read_csv(csv_file)

    count = 0
    for row in tqdm(data, desc="Processing Entities"):
        entity_type = get_entity_type(row['surviving_entity'])
        if row.get('Done') != 'True' and entity_type in entity_types:
            surviving_entity = URIRef(row['surviving_entity'])
            merged_entities = row['merged_entities'].split('; ')
            for merged_entity in merged_entities:
                # if not validate_entity(merged_entity.strip(), triplestore_endpoint):
                #     print(f"Invalid or non-existent merged entity: {merged_entity}")
                #     continue
                try:
                    meta_editor.merge(surviving_entity, URIRef(merged_entity.strip()))
                except ValueError:
                    continue
            row['Done'] = 'True'
            count += 1

            if count >= 10:
                write_csv(csv_file, data)
                count = 0

    if count > 0:
        write_csv(csv_file, data)

def process_rows_wrapper(args):
    data_chunk, csv_file, meta_config, resp_agent, entity_types, triplestore_endpoint, stop_file_path, temp_file_dir, progress_queue = args
    process_rows(data_chunk, csv_file, meta_config, resp_agent, entity_types, triplestore_endpoint, stop_file_path, temp_file_dir, progress_queue)

def process_rows(data_chunk, csv_file, meta_config, resp_agent, entity_types, triplestore_endpoint, stop_file_path, temp_file_dir, progress_queue):
    meta_editor = MetaEditor(meta_config, resp_agent)

    for row in data_chunk:
        if os.path.exists(stop_file_path):
            break

        entity_type = get_entity_type(row['surviving_entity'])
        if row.get('Done') != 'True' and entity_type in entity_types:
            surviving_entity = URIRef(row['surviving_entity'])
            merged_entities = row['merged_entities'].split('; ')

            for merged_entity in merged_entities:
                merged_entity = merged_entity.strip()
                try:
                    meta_editor.merge(surviving_entity, URIRef(merged_entity))
                except ValueError:
                    continue

            row['Done'] = 'True'

    with tempfile.NamedTemporaryFile(dir=temp_file_dir, mode='w', newline='', encoding='utf-8', delete=False, prefix="temp_") as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=data_chunk[0].keys())
        writer.writeheader()
        for row in data_chunk:
            writer.writerow(row)

    progress_queue.put(len(data_chunk))
    
def main():
    parser = argparse.ArgumentParser(description="Merge entities from a CSV file.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file")
    parser.add_argument('meta_config', type=str, help="Meta configuration string")
    parser.add_argument('resp_agent', type=str, help="Responsible agent string")
    parser.add_argument('--triplestore_endpoint', type=str, required=True, help="SPARQL endpoint of the triplestore")
    parser.add_argument('--entity_types', nargs='+', default=['ra', 'br', 'id'], help="Types of entities to merge (ra, br, id)")
    parser.add_argument('--chunk_size', type=int, default=10, help="Number of rows per chunk for processing")
    parser.add_argument('--stop_file', type=str, default="stop.out", help="Path to the stop file")
    parser.add_argument('--num_workers', type=int, default=None, help="Number of worker processes. Defaults to the number of CPU cores if not set")
    args = parser.parse_args()

    if os.path.exists(args.stop_file):
        os.remove(args.stop_file)

    temp_dir_name = "temp_files_for_" + os.path.basename(args.csv_file).split('.')[0]
    temp_file_dir = os.path.join('.', temp_dir_name)

    if not os.path.exists(temp_file_dir):
        os.makedirs(temp_file_dir)

    data = read_csv(args.csv_file)
    if 'br' in args.entity_types:
        classified_data = classify_br_entities(data, args.triplestore_endpoint)
    else:
        classified_data = {0: data}

    with Manager() as manager:
        progress_queue = manager.Queue()
        chunk_args = []

        for group_index in classified_data.keys():
            group_data = classified_data[group_index]
            data_chunks = [group_data[i:i + args.chunk_size] for i in range(0, len(group_data), args.chunk_size)]

            for data_chunk in data_chunks:
                chunk_args.append((data_chunk, args.csv_file, args.meta_config, args.resp_agent, args.entity_types, args.triplestore_endpoint, args.stop_file, temp_file_dir, progress_queue))

        with Pool(args.num_workers) as pool:
            pool.map(process_rows_wrapper, chunk_args)

            total_rows = sum(len(chunk) for chunk in data_chunks)
            with tqdm(total=total_rows) as pbar:
                while not progress_queue.empty():
                    rows_processed = progress_queue.get()
                    pbar.update(rows_processed)

    # Merge temporary files back into the main CSV file
    temp_files = [f for f in os.listdir(temp_file_dir) if f.startswith("temp_")]
    with open(args.csv_file, 'w', newline='', encoding='utf-8') as main_file:
        writer = None
        for temp_file in temp_files:
            with open(os.path.join(temp_file_dir, temp_file), 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                if not writer:
                    writer = csv.DictWriter(main_file, fieldnames=reader.fieldnames)
                    writer.writeheader()
                for row in reader:
                    writer.writerow(row)
            os.remove(os.path.join(temp_file_dir, temp_file))

    shutil.rmtree(temp_file_dir)

if __name__ == "__main__":
    main()