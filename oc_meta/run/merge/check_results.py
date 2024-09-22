import argparse
import csv
import os
import zipfile
from collections import defaultdict

from oc_meta.plugins.editor import MetaEditor
from rdflib import PROV, RDF, ConjunctiveGraph, URIRef
from rdflib.namespace import DCTERMS
from tqdm import tqdm


def read_csv(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def check_entity_exists(file_path, entity_uri):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as file:
                g = ConjunctiveGraph()
                g.parse(file, format='json-ld')
                if (URIRef(entity_uri), None, None) in g:
                    return True
    return False

def check_provenance(prov_file_path, entity_uri, is_merged_entity):
    errors = []
    with zipfile.ZipFile(prov_file_path, 'r') as zip_ref:
        g = ConjunctiveGraph()
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as file:
                g.parse(file, format='json-ld')
    
    entity_graph_uri = URIRef(entity_uri + '/prov/')
    entity_graph = g.get_context(entity_graph_uri)
    
    if not entity_graph:
        errors.append(f"Error in {prov_file_path}: Named graph not found for {entity_uri}")
        return errors

    entities = list(entity_graph.subjects(RDF.type, PROV.Entity))
    if not entities:
        errors.append(f"Error in {prov_file_path}: No entities found in the named graph for {entity_uri}")

    required_properties = [
        PROV.generatedAtTime,
        PROV.specializationOf,
        DCTERMS.description,
        PROV.wasAttributedTo
    ]

    for entity in entities:
        if (entity, RDF.type, PROV.Entity) not in entity_graph:
            errors.append(f"Error in {prov_file_path}: Entity {entity} is not of type prov:Entity in the named graph")

        for prop in required_properties:
            if not list(entity_graph.objects(entity, prop)):
                errors.append(f"Error in {prov_file_path}: Entity {entity} is missing required property {prop} in the named graph")

    if (None, PROV.specializationOf, URIRef(entity_uri)) not in entity_graph:
        errors.append(f"Error in {prov_file_path}: No entity found with prov:specializationOf {entity_uri} in the named graph")

    if is_merged_entity:
        # Check for prov:invalidatedAtTime on the last snapshot
        snapshots = defaultdict(list)
        for s, p, o in entity_graph:
            if '/prov/se/' in str(s):
                snapshot_num = int(str(s).split('/prov/se/')[-1])
                snapshots[snapshot_num].append((s, p, o))
        
        if snapshots:
            last_snapshot_num = max(snapshots.keys())
            last_snapshot = snapshots[last_snapshot_num]
            if not any(p == PROV.invalidatedAtTime for _, p, _ in last_snapshot):
                errors.append(f"Error in {prov_file_path}: Last snapshot {entity_uri}/prov/se/{last_snapshot_num} is missing prov:invalidatedAtTime")

    return errors

def main():
    parser = argparse.ArgumentParser(description="Check merge process success")
    parser.add_argument('csv_folder', type=str, help="Path to the folder containing CSV files")
    parser.add_argument('rdf_dir', type=str, help="Path to the RDF directory")
    parser.add_argument('--meta_config', type=str, required=True, help="Path to meta configuration file")
    args = parser.parse_args()

    meta_editor = MetaEditor(args.meta_config, "")

    csv_files = [f for f in os.listdir(args.csv_folder) if f.endswith('.csv')]
    
    for csv_file in tqdm(csv_files, desc="Processing CSV files"):
        csv_path = os.path.join(args.csv_folder, csv_file)
        data = read_csv(csv_path)

        for row in data:
            if 'Done' not in row or row['Done'] != 'True':
                continue

            surviving_entity = row['surviving_entity']
            merged_entities = row['merged_entities'].split('; ')
            all_entities = [surviving_entity] + merged_entities

            for entity in all_entities:
                file_path = meta_editor.find_file(args.rdf_dir, meta_editor.dir_split, meta_editor.n_file_item, entity, True)
                
                if file_path is None:
                    tqdm.write(f"Error in {csv_path}: Could not find file for entity {entity}")
                    continue

                exists = check_entity_exists(file_path, entity)
                if entity == surviving_entity and not exists:
                    tqdm.write(f"Error in {file_path}: Surviving entity {entity} does not exist")
                elif entity != surviving_entity and exists:
                    tqdm.write(f"Error in {file_path}: Merged entity {entity} still exists")

                # file_path_without_extension = os.path.splitext(file_path)[0]
                # prov_file_path = os.path.join(file_path_without_extension, 'prov', 'se.zip')
                # if not os.path.exists(prov_file_path):
                #     tqdm.write(f"Error: Provenance file not found for {entity} (expected at {prov_file_path})")
                # else:
                #     prov_errors = check_provenance(prov_file_path, entity, entity != surviving_entity)
                #     for error in prov_errors:
                #         tqdm.write(error)

if __name__ == "__main__":
    main()