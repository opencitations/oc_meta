import argparse
import os
import zipfile
import json
import csv
from tqdm import tqdm

def read_and_analyze_zip_files(folder_path, csv_path):
    id_folder_path = os.path.join(folder_path, 'id')
    entity_info = {}

    if not os.path.exists(id_folder_path):
        print(f"La sottocartella 'id' non esiste nel percorso: {folder_path}")
        return

    zip_files = get_zip_files(id_folder_path)

    for zip_path in tqdm(zip_files, desc="Analizzando i file ZIP"):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for zip_file in zip_ref.namelist():
                with zip_ref.open(zip_file) as json_file:
                    data = json.load(json_file)
                    analyze_json(data, entity_info)

    save_duplicates_to_csv(entity_info, csv_path)

def get_zip_files(id_folder_path):
    zip_files = []
    for root, dirs, files in os.walk(id_folder_path):
        for file in files:
            if file.endswith('.zip') and file != 'se.zip':
                zip_files.append(os.path.join(root, file))
    return zip_files

def analyze_json(data, entity_info):
    for graph in data:
        for entity in graph["@graph"]:
            entity_id = entity["@id"]
            identifier_scheme = entity["http://purl.org/spar/datacite/usesIdentifierScheme"][0]["@id"]
            literal_value = entity["http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"][0]["@value"]

            key = (identifier_scheme, literal_value)
            if key not in entity_info:
                entity_info[key] = []
            entity_info[key].append(entity_id)

def save_duplicates_to_csv(entity_info, csv_path):
    with open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['surviving_entity', 'merged_entities'])

        for key, ids in entity_info.items():
            if len(ids) > 1:
                surviving_entity = ids[0]
                merged_entities = '; '.join(ids[1:])
                csv_writer.writerow([surviving_entity, merged_entities])

def main():
    parser = argparse.ArgumentParser(description="Legge i file JSON all'interno dei file ZIP in una sottocartella 'id'.")
    parser.add_argument("folder_path", type=str, help="Percorso della cartella contenente la sottocartella 'id'")
    parser.add_argument("csv_path", type=str, help="Percorso del file CSV per salvare i duplicati")
    args = parser.parse_args()

    read_and_analyze_zip_files(args.folder_path, args.csv_path)

if __name__ == "__main__":
    main()