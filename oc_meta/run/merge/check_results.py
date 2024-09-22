import argparse
import csv
import os
import zipfile
from functools import partial
from multiprocessing import Pool, cpu_count

from oc_meta.plugins.editor import MetaEditor
from rdflib import RDF, ConjunctiveGraph, Literal, URIRef
from tqdm import tqdm

DATACITE = "http://purl.org/spar/datacite/"
LITERAL_REIFICATION = "http://www.essepuntato.it/2010/06/literalreification/"

def read_csv(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def check_entity(file_path, entity_uri, is_surviving):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as file:
                g = ConjunctiveGraph()
                g.parse(file, format='json-ld')
                entity = URIRef(entity_uri)
                
                if (entity, None, None) not in g:
                    if is_surviving:
                        tqdm.write(f"Error in {file_path}: Surviving entity {entity_uri} does not exist")
                    return
                
                if not is_surviving:
                    tqdm.write(f"Error in {file_path}: Merged entity {entity_uri} still exists")
                    return
                
                types = list(g.objects(entity, RDF.type))
                if not types:
                    tqdm.write(f"Error in {file_path}: Entity {entity_uri} has no type")
                
                if URIRef(DATACITE + "Identifier") in types:
                    identifier_scheme = list(g.objects(entity, URIRef(DATACITE + "usesIdentifierScheme")))
                    literal_value = list(g.objects(entity, URIRef(LITERAL_REIFICATION + "hasLiteralValue")))
                    
                    if len(identifier_scheme) != 1:
                        tqdm.write(f"Error in {file_path}: Entity {entity_uri} should have exactly one usesIdentifierScheme, found {len(identifier_scheme)}")
                    elif not isinstance(identifier_scheme[0], URIRef):
                        tqdm.write(f"Error in {file_path}: Entity {entity_uri}'s usesIdentifierScheme should be a URIRef, found {type(identifier_scheme[0])}")
                    
                    if len(literal_value) != 1:
                        tqdm.write(f"Error in {file_path}: Entity {entity_uri} should have exactly one hasLiteralValue, found {len(literal_value)}")
                    elif not isinstance(literal_value[0], Literal):
                        tqdm.write(f"Error in {file_path}: Entity {entity_uri}'s hasLiteralValue should be a Literal, found {type(literal_value[0])}")

def process_csv(args, csv_file):
    csv_path, rdf_dir, meta_editor = args
    csv_path = os.path.join(csv_path, csv_file)
    data = read_csv(csv_path)
    tasks = []

    for row in data:
        if 'Done' not in row or row['Done'] != 'True':
            continue

        surviving_entity = row['surviving_entity']
        merged_entities = row['merged_entities'].split('; ')
        all_entities = [surviving_entity] + merged_entities

        for entity in all_entities:
            file_path = meta_editor.find_file(rdf_dir, meta_editor.dir_split, meta_editor.n_file_item, entity, True)
            tasks.append((entity, entity == surviving_entity, file_path, rdf_dir, meta_editor))

    return tasks

def process_entity(args):
    entity, is_surviving, file_path, rdf_dir, meta_editor = args

    if file_path is None:
        tqdm.write(f"Error: Could not find file for entity {entity}")
        return

    check_entity(file_path, entity, is_surviving)

def main():
    parser = argparse.ArgumentParser(description="Check merge process success")
    parser.add_argument('csv_folder', type=str, help="Path to the folder containing CSV files")
    parser.add_argument('rdf_dir', type=str, help="Path to the RDF directory")
    parser.add_argument('--meta_config', type=str, required=True, help="Path to meta configuration file")
    args = parser.parse_args()

    meta_editor = MetaEditor(args.meta_config, "")

    csv_files = [f for f in os.listdir(args.csv_folder) if f.endswith('.csv')]
    
    # Processamento dei file CSV in parallelo
    with Pool(processes=cpu_count()) as pool:
        process_csv_partial = partial(process_csv, (args.csv_folder, args.rdf_dir, meta_editor))
        all_tasks = list(tqdm(pool.imap(process_csv_partial, csv_files), total=len(csv_files), desc="Processing CSV files"))
    
    # Appiattire la lista di liste in una singola lista
    all_tasks = [task for sublist in all_tasks for task in sublist]

    # Processamento delle entità in parallelo
    with Pool(processes=cpu_count()) as pool:
        list(tqdm(pool.imap(process_entity, all_tasks), total=len(all_tasks), desc="Processing entities"))

if __name__ == "__main__":
    main()