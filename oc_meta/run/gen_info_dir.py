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

from __future__ import annotations

import argparse
import json
import os
import zipfile
from concurrent.futures import TimeoutError, as_completed

from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.support import get_prefix, get_resource_number, get_short_name
from pebble import ProcessPool
from tqdm import tqdm


def find_max_numbered_folder(path):
    """
    Trova la sottocartella con il numero più elevato in una data cartella.
    """
    max_number = -1
    for folder in os.listdir(path):
        if folder.isdigit():
            max_number = max(max_number, int(folder))
    return max_number

def find_max_numbered_zip_file(folder_path):
    """
    Trova il file zippato con il numero più elevato prima di ".zip" all'interno di una cartella.
    """
    max_number = -1
    max_zip_file = None

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
            json_data = json.load(entity_file)
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

def explore_directories(root_path, redis_host, redis_port, redis_db):
    """
    Esplora le directory e associa a ciascun supplier prefix il numero maggiore.
    """
    main_folders = ["ar", "br", "ra", "re", "id"]
    counter_handler = RedisCounterHandler(host=redis_host, port=redis_port, db=redis_db)

    for main_folder in main_folders:
        main_folder_path = os.path.join(root_path, main_folder)
        if os.path.isdir(main_folder_path):
            for supplier_prefix in os.listdir(main_folder_path):
                supplier_path = os.path.join(main_folder_path, supplier_prefix)
                max_folder = find_max_numbered_folder(supplier_path)
                max_zip_file = find_max_numbered_zip_file(os.path.join(supplier_path, str(max_folder)))
                zip_file_path = os.path.join(supplier_path, str(max_folder), max_zip_file)
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    first_file = zip_ref.namelist()[0]
                    with zip_ref.open(first_file) as entity_file:
                        json_data = json.load(entity_file)
                        max_entity = -1
                        for graph in json_data:
                            for entity in graph['@graph']:
                                entity_uri = entity['@id']
                                resource_number = get_resource_number(entity_uri)
                                max_entity = max(max_entity, resource_number)

                counter_handler.set_counter(max_entity, main_folder, supplier_prefix=supplier_prefix)
    
    zip_files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(root_path) 
                 for f in filenames if f.endswith('.zip') and 'prov' in dp]
    
    pbar = tqdm(total=len(zip_files))
    results = []

    timeout = 30

    with ProcessPool() as pool:
        future_results = {pool.schedule(process_zip_file, args=[zip_file], timeout=timeout): zip_file 
                          for zip_file in zip_files}

        results = []
        with tqdm(total=len(zip_files)) as pbar:
            for future in as_completed(future_results):
                zip_file = future_results[future]
                try:
                    result = future.result()
                    results.append(result)
                except TimeoutError:
                    print(f"Processo eseguito oltre il timeout di {timeout} secondi per il file: {zip_file}")
                except Exception as e:
                    print(f"Errore nell'elaborazione del file {zip_file}: {e}")
                finally:
                    pbar.update(1)

    final_batch_updates = {}
    with tqdm(total=len(results), desc="Fusione dei risultati") as pbar:
        for batch in results:
            for supplier_prefix, value in batch.items():
                if supplier_prefix not in final_batch_updates:
                    final_batch_updates[supplier_prefix] = value
                else:
                    for batch_key, inner_value in value.items():
                        if batch_key in final_batch_updates[supplier_prefix]:
                            final_batch_updates[supplier_prefix][batch_key].update(inner_value)
                        else:
                            final_batch_updates[supplier_prefix][batch_key] = inner_value
            pbar.update(1)

    for supplier_prefix, value in final_batch_updates.items():
        for (short_name, prov_short_name), counters in value.items():
            for identifier, counter_value in counters.items():
                counter_handler.set_counter(counter_value, short_name, prov_short_name, identifier, supplier_prefix)

def main():
    parser = argparse.ArgumentParser(description="Esplora le directory e trova i numeri massimi.")
    parser.add_argument("directory", type=str, help="Il percorso della directory da esplorare")
    parser.add_argument("--redis-host", type=str, default="localhost", help="L'host del server Redis")
    parser.add_argument("--redis-port", type=int, default=6379, help="La porta del server Redis")
    parser.add_argument("--redis-db", type=int, default=0, help="Il numero del database Redis da utilizzare")
    args = parser.parse_args()

    explore_directories(args.directory, args.redis_host, args.redis_port, args.redis_db)

if __name__ == "__main__":
    main()