#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import zipfile
from collections import defaultdict
from tqdm import tqdm

import rdflib


def merge_file_set(identifier, file_paths, zip_output_rdf):
    """
    Merges partial RDF files into a single final file with progress tracking
    """
    try:
        merged_graph = rdflib.ConjunctiveGraph()
        base_file_path = f"{identifier}.json" if not zip_output_rdf else f"{identifier}.zip"

        if os.path.exists(base_file_path):
            if zip_output_rdf:
                with zipfile.ZipFile(base_file_path, 'r') as zipf:
                    with zipf.open(zipf.namelist()[0], 'r') as json_file:
                        base_graph_data = json.load(json_file)
                        merged_graph.parse(data=json.dumps(base_graph_data), format='json-ld')
            else:
                with open(base_file_path, 'r', encoding='utf-8') as json_file:
                    base_graph_data = json.load(json_file)
                    merged_graph.parse(data=json.dumps(base_graph_data), format='json-ld')
        for file_path in file_paths:
            if zip_output_rdf:
                with zipfile.ZipFile(file_path, 'r') as zipf:
                    with zipf.open(zipf.namelist()[0], 'r') as json_file:
                        graph_data = json.load(json_file)
                        merged_graph.parse(data=json.dumps(graph_data), format='json-ld')
            else:
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    graph_data = json.load(json_file)
                    merged_graph.parse(data=json.dumps(graph_data), format='json-ld')

        if zip_output_rdf:
            with zipfile.ZipFile(base_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr(f"{os.path.basename(identifier)}.json", 
                            merged_graph.serialize(format='json-ld', indent=None, ensure_ascii=False).encode('utf-8'))
        else:
            with open(base_file_path, 'w', encoding='utf-8') as f:
                f.write(merged_graph.serialize(format='json-ld', indent=None, ensure_ascii=False))

        for file_path in file_paths:
            os.remove(file_path)

        print(f"Merge completed for: {base_file_path}")
    except Exception as e:
        print(f"Error during file merge for {identifier}: {e}")


def find_unmerged_files(base_dir):
    """
    Recursively explores base_dir to find unmerged partial RDF files
    """
    files_to_merge = defaultdict(list)

    for root, dirs, files in os.walk(base_dir):
        json_files = [f for f in files if f.endswith('.json')]
        zip_files = [f for f in files if f.endswith('.zip')]

        for file in json_files:
            if '_' in file:
                number_part = file.split('_')[0]
                identifier = os.path.join(root, number_part)
                files_to_merge[identifier].append(os.path.join(root, file))

        for file in zip_files:
            if '_' in file:
                number_part = file.split('_')[0]
                identifier = os.path.join(root, number_part)
                files_to_merge[identifier].append(os.path.join(root, file))

    final_candidates = {}
    for identifier, partial_files in files_to_merge.items():
        zip_output_rdf = any(pf.endswith('.zip') for pf in partial_files)
        final_file = f"{identifier}.{'zip' if zip_output_rdf else 'json'}"
        
        if not os.path.exists(final_file) or partial_files:
            final_candidates[identifier] = (partial_files, zip_output_rdf)

    return final_candidates


def main():
    parser = argparse.ArgumentParser(description='Finds and merges unmerged RDF files.')
    parser.add_argument('base_dir', help='Base directory to search for unmerged files.')
    args = parser.parse_args()

    anomalies = find_unmerged_files(args.base_dir)
    if not anomalies:
        print("No anomalies found.")
    else:
        print(f"Found {len(anomalies)} sets of files to merge:")
        for identifier, (partial_files, zip_output_rdf) in tqdm(anomalies.items(), desc="Processing file sets", unit="set"):
            merge_file_set(identifier, partial_files, zip_output_rdf)


if __name__ == "__main__":
    main()