import argparse
import zipfile
import os
import json
from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from oc_meta.run.align_rdf_with_triplestore import find_paths
from functools import partial

BASE_IRI = "https://w3id.org/oc/meta/"
DIR_SPLIT = 10000
N_FILE_ITEM = 1000
IS_JSON = True

def extract_and_process_json(zip_path, source_folder):
    processed_zip_cache = {}
    changes_made = False
    with zipfile.ZipFile(zip_path, 'r') as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.json'):
                with z.open(file_info.filename) as file:
                    json_data = json.load(file)
                    graph = ConjunctiveGraph()
                    graph.parse(data=json.dumps(json_data), format='json-ld')
                    for subject in graph.subjects(unique=True):
                        current_context = URIRef(str(subject).split('/prov/')[0] + '/prov/')
                        if not (subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), None) in graph:
                            _, cur_file_path = find_paths(subject, source_folder, BASE_IRI, "_", DIR_SPLIT, N_FILE_ITEM, IS_JSON)
                            cur_file_path = cur_file_path.replace('.json', '.zip')
                            if cur_file_path not in processed_zip_cache:
                                if os.path.exists(cur_file_path):
                                    with zipfile.ZipFile(cur_file_path, 'r') as cur_zip:
                                        for cur_file_info in cur_zip.infolist():
                                            if cur_file_info.filename.endswith('.json'):
                                                with cur_zip.open(cur_file_info.filename) as cur_file:
                                                    cur_json_data = json.load(cur_file)
                                                    cur_graph = ConjunctiveGraph()
                                                    cur_graph.parse(data=json.dumps(cur_json_data), format='json-ld')
                                                    processed_zip_cache[cur_file_path] = cur_graph
                            if cur_file_path in processed_zip_cache:
                                cur_graph: ConjunctiveGraph = processed_zip_cache[cur_file_path]
                                # Check for the prov#hadPrimarySource predicate
                                for o in cur_graph.objects(subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), unique=True):
                                    if str(o) == 'https://api.crossref.org/':
                                        graph.add((subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), URIRef('https://api.crossref.org/snapshots/monthly/2022/12/all.json.tar.gz'), current_context))
                                        changes_made = True
                                    elif str(o) == 'https://api.datacite.org/':
                                        graph.add((subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), URIRef('https://archive.org/details/datacite_dump_20211022'), current_context))
                                        changes_made = True
                                    elif str(o) == 'https://nih.figshare.com/collections/iCite_Database_Snapshots_NIH_Open_Citation_Collection_/4586573/42':
                                        graph.add((subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), URIRef('https://nih.figshare.com/collections/iCite_Database_Snapshots_NIH_Open_Citation_Collection_/4586573/36'), current_context))
                                        changes_made = True
                                    elif str(o) == 'https://api.crossref.org/snapshots/monthly/2023/09/all.json.tar.gz':
                                        graph.add((subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), URIRef('https://api.crossref.org/snapshots/monthly/2023/09/all.json.tar.gz'), current_context))
                                        changes_made = True
                                    elif str(o) == 'https://doi.org/10.5281/zenodo.7845968':
                                        graph.add((subject, URIRef("http://www.w3.org/ns/prov#hadPrimarySource"), URIRef('https://doi.org/10.5281/zenodo.7845968'), current_context))
                                        changes_made = True
                                    else:
                                        print(o)
    if changes_made:
        updated_json = graph.serialize(format='json-ld', indent=None, encoding='utf-8', ensure_ascii=False)
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as z:
            z.writestr('se.json', updated_json)

def process_zip_files(zip_files, source_folder):
    func = partial(extract_and_process_json, source_folder=source_folder)
    with Pool(processes=cpu_count()) as pool:
        list(tqdm(pool.imap(func, zip_files), total=len(zip_files), desc="Processing se.zip files"))

def find_zip_files(destination_folder, source_folder):
    count_file = 'se_zip_file_paths.json'
    se_zip_files = []

    if os.path.exists(count_file):
        with open(count_file, 'r') as f:
            se_zip_files = json.load(f)
            if not se_zip_files:
                print("No se.zip files to process.")
                return
    else:
        for root, dirs, files in os.walk(destination_folder):
            for file in files:
                if file == 'se.zip':
                    se_zip_files.append(os.path.join(root, file))
        
        with open(count_file, 'w') as f:
            json.dump(se_zip_files, f)

    if se_zip_files:
        process_zip_files(se_zip_files, source_folder)
    
    if os.path.exists(count_file):
        os.remove(count_file)

def main():
    parser = argparse.ArgumentParser(description="Process se.zip files containing JSON-LD with RDF data.")
    parser.add_argument("destination_folder", type=str, help="Folder where to find se.zip files.")
    parser.add_argument("source_folder", type=str, help="Folder to store outputs (not used in this script).")
    args = parser.parse_args()

    find_zip_files(args.destination_folder, args.source_folder)

if __name__ == "__main__":
    main()