import argparse
import csv
from oc_meta.plugins.editor import MetaEditor
from rdflib import URIRef
from tqdm import tqdm
import os


def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

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

def process_rows(data, csv_file, meta_config, resp_agent, entity_types, stop_file_path):
    meta_editor = MetaEditor(meta_config, resp_agent, save_queries=True)
    count = 0

    for row in tqdm(data, desc="Processing Entities"):
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
            count += 1

            if count >= 10:
                write_csv(csv_file, data)
                count = 0

    if count > 0:
        write_csv(csv_file, data)

def main():
    parser = argparse.ArgumentParser(description="Merge entities from a CSV file.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file")
    parser.add_argument('meta_config', type=str, help="Meta configuration string")
    parser.add_argument('resp_agent', type=str, help="Responsible agent string")
    parser.add_argument('--entity_types', nargs='+', default=['ra', 'br', 'id'], help="Types of entities to merge (ra, br, id)")
    parser.add_argument('--stop_file', type=str, default="stop.out", help="Path to the stop file")
    args = parser.parse_args()

    if os.path.exists(args.stop_file):
        os.remove(args.stop_file)

    data = read_csv(args.csv_file)
    process_rows(data, args.csv_file, args.meta_config, args.resp_agent, args.entity_types, args.stop_file)

if __name__ == "__main__":
    main()