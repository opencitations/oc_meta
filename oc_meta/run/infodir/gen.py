#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import multiprocessing
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.support import get_prefix, get_resource_number, get_short_name
from rich_argparse import RichHelpFormatter
from tqdm import tqdm

from oc_meta.lib.file_manager import collect_zip_files


def find_max_numbered_folder(path):
    max_number = -1
    for folder in os.listdir(path):
        if folder.isdigit():
            max_number = max(max_number, int(folder))
    return max_number

def find_max_numbered_zip_file(folder_path: str) -> str | None:
    max_number = -1
    max_zip_file: str | None = None

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".zip"):
            prefix, extension = os.path.splitext(file_name)
            if prefix.isdigit():
                number = int(prefix)
                if number > max_number:
                    max_number = number
                    max_zip_file = file_name
    return max_zip_file

def process_zip_file(zip_file_path):
    batch_updates = {}
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        first_file = zip_ref.namelist()[0]
        with zip_ref.open(first_file) as entity_file:
            json_data = orjson.loads(entity_file.read())
            for graph in json_data:
                for entity in graph['@graph']:
                    prov_entity_uri = entity['@id']
                    entity_uri = entity['@id'].split('/prov/se/')[0]
                    supplier_prefix = get_prefix(entity_uri)
                    resource_number = get_resource_number(entity_uri)
                    short_name = get_short_name(entity_uri)
                    prov_short_name = 'se'
                    counter_value = int(prov_entity_uri.split('/prov/se/')[-1])
                    batch_key = (short_name, prov_short_name)
                    if supplier_prefix not in batch_updates:
                        batch_updates[supplier_prefix] = dict()
                    if batch_key not in batch_updates[supplier_prefix]:
                        batch_updates[supplier_prefix][batch_key] = dict()
                    # Save the maximum counter value for each entity
                    if resource_number not in batch_updates[supplier_prefix][batch_key]:
                        batch_updates[supplier_prefix][batch_key][resource_number] = counter_value
                    else:
                        batch_updates[supplier_prefix][batch_key][resource_number] = max(
                            batch_updates[supplier_prefix][batch_key][resource_number], counter_value)
    return batch_updates

SUPPLIER_PREFIX = "060"


def explore_directories(root_path, info_dir):
    main_folders = ["ar", "br", "ra", "re", "id"]
    info_dir_with_prefix = os.path.join(info_dir, SUPPLIER_PREFIX) + os.sep
    counter_handler = FilesystemCounterHandler(info_dir=info_dir_with_prefix, supplier_prefix=SUPPLIER_PREFIX)

    for main_folder in main_folders:
        main_folder_path = os.path.join(root_path, main_folder)
        if os.path.isdir(main_folder_path):
            for supplier_prefix in os.listdir(main_folder_path):
                supplier_path = os.path.join(main_folder_path, supplier_prefix)
                max_folder = find_max_numbered_folder(supplier_path)
                max_zip_file = find_max_numbered_zip_file(os.path.join(supplier_path, str(max_folder)))
                if max_zip_file is None:
                    continue
                zip_file_path = os.path.join(supplier_path, str(max_folder), max_zip_file)
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    first_file = zip_ref.namelist()[0]
                    with zip_ref.open(first_file) as entity_file:
                        json_data = orjson.loads(entity_file.read())
                        max_entity = -1
                        for graph in json_data:
                            for entity in graph['@graph']:
                                entity_uri = entity['@id']
                                resource_number = get_resource_number(entity_uri)
                                max_entity = max(max_entity, resource_number)

                counter_handler.set_counter(max_entity, main_folder, supplier_prefix=supplier_prefix)
    
    zip_files = collect_zip_files(root_path, only_prov=True)
    
    # Use forkserver to avoid deadlocks when forking in a multi-threaded environment
    ctx = multiprocessing.get_context('forkserver')
    with ProcessPoolExecutor(mp_context=ctx) as executor:
        future_results = {executor.submit(process_zip_file, zip_file): zip_file
                          for zip_file in zip_files}

        results = []
        with tqdm(total=len(zip_files), desc="Processing provenance zip files") as pbar:
            for future in as_completed(future_results):
                zip_file = future_results[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Error processing file {zip_file}: {e}")
                finally:
                    pbar.update(1)

    final_batch_updates = {}
    with tqdm(total=len(results), desc="Merging results") as pbar:
        for batch in results:
            for supplier_prefix, value in batch.items():
                if supplier_prefix not in final_batch_updates:
                    final_batch_updates[supplier_prefix] = value
                else:
                    for batch_key, inner_value in value.items():
                        if batch_key in final_batch_updates[supplier_prefix]:
                            for identifier, counter_value in inner_value.items():
                                current_value = final_batch_updates[supplier_prefix][batch_key].get(identifier, 0)
                                final_batch_updates[supplier_prefix][batch_key][identifier] = max(current_value, counter_value)
                        else:
                            final_batch_updates[supplier_prefix][batch_key] = inner_value
            pbar.update(1)

    for prefix, updates in final_batch_updates.items():
        counter_handler.set_counters_batch(updates, prefix)

def main():
    parser = argparse.ArgumentParser(
        description="Scan RDF directories and populate filesystem counter files.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("directory", type=str, help="Path to the RDF directory to scan")
    parser.add_argument("info_dir", type=str, help="Base directory for counter files")
    args = parser.parse_args()

    explore_directories(args.directory, args.info_dir)

if __name__ == "__main__":
    main()