import argparse
import os
import zipfile
import json
import csv
from tqdm import tqdm
import logging

logging.basicConfig(filename='error_log_find_duplicated_resources.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, item):
        if item not in self.parent:
            self.parent[item] = item
            self.rank[item] = 0
            return item
        
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, x, y):
        xroot = self.find(x)
        yroot = self.find(y)

        if xroot == yroot:
            return

        if self.rank[xroot] < self.rank[yroot]:
            self.parent[xroot] = yroot
        elif self.rank[xroot] > self.rank[yroot]:
            self.parent[yroot] = xroot
        else:
            self.parent[yroot] = xroot
            self.rank[xroot] += 1

def read_and_analyze_zip_files(folder_path, csv_path, resource_type):
    resources = {}
    
    if resource_type in ['br', 'both']:
        br_folder_path = os.path.join(folder_path, 'br')
        process_folder(br_folder_path, resources, 'br')
    
    if resource_type in ['ra', 'both']:
        ra_folder_path = os.path.join(folder_path, 'ra')
        process_folder(ra_folder_path, resources, 'ra')

    save_duplicates_to_csv(resources, csv_path)

def process_folder(folder_path, resources, expected_type):
    if not os.path.exists(folder_path):
        logging.error(f"La sottocartella '{expected_type}' non esiste nel percorso: {folder_path}")
        return

    zip_files = get_zip_files(folder_path)

    for zip_path in tqdm(zip_files, desc=f"Analizzando i file ZIP in {expected_type}"):
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for zip_file in zip_ref.namelist():
                    try:
                        with zip_ref.open(zip_file) as json_file:
                            data = json.load(json_file)
                            analyze_json(data, resources, zip_path, zip_file, expected_type)
                    except json.JSONDecodeError:
                        logging.error(f"Errore nel parsing JSON del file {zip_file} in {zip_path}")
                    except Exception as e:
                        logging.error(f"Errore nell'elaborazione del file {zip_file} in {zip_path}: {str(e)}")
        except zipfile.BadZipFile:
            logging.error(f"File ZIP corrotto o non valido: {zip_path}")
        except Exception as e:
            logging.error(f"Errore nell'apertura del file ZIP {zip_path}: {str(e)}")

def get_zip_files(folder_path):
    zip_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.zip') and not file == 'se.zip':
                zip_files.append(os.path.join(root, file))
    return zip_files

def analyze_json(data, resources, zip_path, zip_file, expected_type):
    for graph in data:
        for entity in graph.get("@graph", []):
            try:
                entity_id = entity["@id"]
                entity_type = get_entity_type(entity)
                
                if entity_type is None:
                    print(f"Tipo non specificato per l'entità {entity_id} nel file {zip_file} all'interno di {zip_path}. Assumendo tipo {expected_type}.")
                    entity_type = expected_type
                
                if entity_type == expected_type:
                    identifiers = get_identifiers(entity)
                    
                    if entity_id not in resources:
                        resources[entity_id] = set()
                    resources[entity_id].update(identifiers)
            except KeyError as e:
                logging.error(f"Chiave mancante nell'entità {entity.get('@id', 'ID sconosciuto')} "
                              f"nel file {zip_file} all'interno di {zip_path}: {str(e)}")
            except Exception as e:
                logging.error(f"Errore nell'analisi dell'entità {entity.get('@id', 'ID sconosciuto')} "
                              f"nel file {zip_file} all'interno di {zip_path}: {str(e)}")

def get_entity_type(entity):
    if "http://purl.org/spar/fabio/Expression" in entity.get("@type", []):
        return 'br'
    elif "http://xmlns.com/foaf/0.1/Agent" in entity.get("@type", []):
        return 'ra'
    return None

def get_identifiers(entity):
    identifiers = []
    for identifier in entity.get("http://purl.org/spar/datacite/hasIdentifier", []):
        if isinstance(identifier, dict) and "@id" in identifier:
            identifiers.append(identifier["@id"])
    return identifiers

def save_duplicates_to_csv(resources, csv_path):
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['surviving_entity', 'merged_entities'])

            duplicates = find_duplicates(resources)
            for group in duplicates:
                surviving_entity = group[0]
                merged_entities = '; '.join(group[1:])
                csv_writer.writerow([surviving_entity, merged_entities])
    except Exception as e:
        logging.error(f"Errore nel salvataggio del file CSV {csv_path}: {str(e)}")

def find_duplicates(resources):
    uf = UnionFind()
    
    # First, create sets of identifiers for each entity
    for entity, identifiers in resources.items():
        for identifier in identifiers:
            uf.union(entity, identifier)
    
    # Then, group entities by their representative
    groups = {}
    for entity in resources:
        rep = uf.find(entity)
        if rep not in groups:
            groups[rep] = []
        groups[rep].append(entity)
    
    # Filter out groups with only one entity
    return [sorted(group) for group in groups.values() if len(group) > 1]

def main():
    parser = argparse.ArgumentParser(description="Trova risorse duplicate in base ai loro ID.")
    parser.add_argument("folder_path", type=str, help="Percorso della cartella contenente le sottocartelle 'br' e 'ra'")
    parser.add_argument("csv_path", type=str, help="Percorso del file CSV per salvare i duplicati")
    parser.add_argument("resource_type", type=str, choices=['br', 'ra', 'both'], 
                        help="Tipo di risorsa da analizzare: 'br' per risorse bibliografiche, 'ra' per agenti responsabili, 'both' per entrambi")
    args = parser.parse_args()

    read_and_analyze_zip_files(args.folder_path, args.csv_path, args.resource_type)

if __name__ == "__main__":
    main()