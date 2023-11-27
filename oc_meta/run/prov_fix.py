#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2023 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE

import argparse
import json
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from multiprocessing import Manager, Queue

from tqdm import tqdm


def find_latest_snapshot(graph):
    max_num = -1
    for entity in graph["@graph"]:
        id = entity["@id"]
        num = int(id.split('/')[-1])
        if num > max_num:
            max_num = num
    return max_num

def check_snapshots(graph, max_num):
    expected_snapshots = set(range(1, max_num + 1))
    actual_snapshots = set()

    for entity in graph["@graph"]:
        num = int(entity["@id"].split('/')[-1])
        actual_snapshots.add(num)

    missing = expected_snapshots - actual_snapshots
    return missing

def check_predicates(graph, exclude_latest_num):
    required_predicates = {
        "http://purl.org/dc/terms/description",
        "http://www.w3.org/ns/prov#generatedAtTime",
        "http://www.w3.org/ns/prov#invalidatedAtTime",
        "http://www.w3.org/ns/prov#hadPrimarySource",
        "http://www.w3.org/ns/prov#specializationOf",
        "http://www.w3.org/ns/prov#wasAttributedTo"
    }

    for entity in graph["@graph"]:
        num = int(entity["@id"].split('/')[-1])
        if num != exclude_latest_num:
            for predicate in required_predicates:
                if predicate not in entity:
                    return False
    return True


def process_zip_file(zip_path, queue):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as file:
                json_data = json.load(file)
                for graph in json_data:
                    # Aggiunta del controllo per snapshot 1
                    snapshot_one_has_primary_source = False
                    for entity in graph["@graph"]:
                        if int(entity["@id"].split('/')[-1]) == 1:
                            if "http://www.w3.org/ns/prov#hadPrimarySource" in entity:
                                snapshot_one_has_primary_source = True
                            break
                    if not snapshot_one_has_primary_source:
                        if entity["@id"].split('/')[5] != 'ar':
                            print(entity["@id"].split('/')[5])
                        queue.put(1)
                    # max_num = find_latest_snapshot(graph)
                    # missing_snapshots = check_snapshots(graph, max_num)
                    # all_predicates = check_predicates(graph, max_num)
                    # if not all_predicates or missing_snapshots:
                    #     queue.put(1)
                    #     entity_id = graph["@id"].split('/prov/')[0]
                    #     new_graph = {
                    #         "@graph": [
                    #             {
                    #                 "@id": f"{entity_id}/prov/se/1",
                    #                 "@type": ["http://www.w3.org/ns/prov#Entity"],
                    #                 "http://purl.org/dc/terms/description": [{"@value": f"The entity '{entity_id}' has been created."}],
                    #                 "http://www.w3.org/ns/prov#generatedAtTime": [{"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}],
                    #                 "http://www.w3.org/ns/prov#specializationOf": [{"@id": entity_id}],
                    #                 "http://www.w3.org/ns/prov#wasAttributedTo": [{"@id": "https://w3id.org/oc/meta/prov/pa/1"}]
                    #             }
                    #         ],
                    #         "@id": entity_id
                    #     }
                        # print(json.dumps(new_graph, indent=4))
                    # elif not all_predicates and not missing_snapshots:
                    #     print('ahi')
                        # print(f"Missing predicates in snapshots in {zip_path}: {graph}", '\n\n')

def find_zip_files_in_subdir(subdir, process_immediately=False):
    if process_immediately:
        if os.path.exists(subdir):
            for root, dirs, files in os.walk(subdir):
                if 'prov' in root.split(os.sep):
                    for file in files:
                        if file.endswith('.zip'):
                            process_zip_file(os.path.join(root, file))
    else:
        zip_files = []
        if os.path.exists(subdir):
            for root, dirs, files in os.walk(subdir):
                if 'prov' in root.split(os.sep):
                    for file in files:
                        if file.endswith('.zip'):
                            zip_files.append(os.path.join(root, file))
        return zip_files

def find_subdirs(directory):
    subdirs = []
    for dir in os.listdir(directory):
        subdirs.append(os.path.join(directory, dir))
    return subdirs

def search_zip_files(directory, show_progress, queue):
    main_subdirs = ['ar', 'br', 'ra', 're', 'id']
    subdirs = []

    # Trova tutte le sottocartelle nelle cartelle principali
    for main_subdir in main_subdirs:
        main_subdir_path = os.path.join(directory, main_subdir)
        subdirs.extend(find_subdirs(main_subdir_path))

    if show_progress:
        zip_files = []
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(find_zip_files_in_subdir, subdir) for subdir in subdirs]
            for future in futures:
                zip_files.extend(future.result())

        with ProcessPoolExecutor() as executor:
            list(tqdm(executor.map(process_zip_file, zip_files, [queue] * len(zip_files)), total=len(zip_files), desc="Processing ZIP files"))
    else:
        for subdir in subdirs:
            find_zip_files_in_subdir(subdir, process_immediately=True)

def main():
    with Manager() as manager:
        queue = manager.Queue()
        parser = argparse.ArgumentParser(description="Process JSON-LD files within ZIP archives")
        parser.add_argument("directory", help="Directory to search for ZIP files")
        parser.add_argument("--progress", help="Show progress bar", action="store_true")
        args = parser.parse_args()
        search_zip_files(args.directory, args.progress, queue)
        corrupted_snapshots_count = 0
        while not queue.empty():
            corrupted_snapshots_count += queue.get()
        print(f"Total number of corrupted snapshots: {corrupted_snapshots_count}")

if __name__ == "__main__":
    main()