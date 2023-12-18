import argparse
import csv
from oc_meta.plugins.editor import MetaEditor
from rdflib import URIRef
from tqdm import tqdm
import signal
from SPARQLWrapper import SPARQLWrapper, JSON

interrupted = False

def signal_handler(signum, frame):
    global interrupted
    interrupted = True

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

def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

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
    global interrupted
    meta_editor = MetaEditor(meta_config, resp_agent)
    data = read_csv(csv_file)

    count = 0
    for row in tqdm(data, desc="Processing Entities"):
        if interrupted:
            print("Processo interrotto. Salvataggio dei dati in corso...")
            write_csv(csv_file, data)
            break
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

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Merge entities from a CSV file.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file")
    parser.add_argument('meta_config', type=str, help="Meta configuration string")
    parser.add_argument('resp_agent', type=str, help="Responsible agent string")
    parser.add_argument('--triplestore_endpoint', type=str, required=True, help="SPARQL endpoint of the triplestore")
    parser.add_argument('--entity_types', nargs='+', default=['ra', 'br'], help="Types of entities to merge (ra, br)")
    args = parser.parse_args()

    merge_entities(args.csv_file, args.meta_config, args.resp_agent, args.entity_types, args.triplestore_endpoint)

if __name__ == "__main__":
    main()
